package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConfStrategy2
import com.hurence.logisland.record.{EvoaUtils, TimeSeriesRecord}
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.lucidworks.spark.util.SolrSupport
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => f}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{WrappedArray => ArrayDF}

class ChunkCompactorJobStrategy2(options: ChunkCompactorConfStrategy2) extends ChunkCompactor {

  private val logger = LoggerFactory.getLogger(classOf[ChunkCompactorJobStrategy2])
  private val point_in_day = "number_points_day"

  private implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord]

  private def loadDataFromSolR(spark: SparkSession): DataFrame = {

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName,
   /*   "splits" -> "true",
      "split_field"-> "name",
      "splits_per_shard"-> "50",*/
      "sort" -> "chunk_start asc",
      "fields" -> "name,chunk_value,chunk_start,chunk_end,chunk_size,year,month,day",
      "filters" -> s"chunk_origin:${options.chunkOriginToCompact}"
    )
    logger.info(s"$solrOpts")

    spark.read
      .format("solr")
      .options(solrOpts)
      .load
  }

  private def saveNewChunksToSolR(timeseriesDS: Dataset[TimeSeriesRecord]) = {


    import timeseriesDS.sparkSession.implicits._

    logger.info(s"start saving new chunks to ${options.collectionName}")
    val savedDF = timeseriesDS
      .map(r => (
        r.getId,
        r.getField(TimeSeriesRecord.CHUNK_YEAR).asInteger(),
        r.getField(TimeSeriesRecord.CHUNK_MONTH).asInteger(),
        r.getField(TimeSeriesRecord.CHUNK_DAY).asInteger(),
        r.getField(TimeSeriesRecord.CODE_INSTALL).asString(),
        r.getField(TimeSeriesRecord.SENSOR).asString(),
        r.getField(TimeSeriesRecord.METRIC_NAME).asString(),
        r.getField(TimeSeriesRecord.CHUNK_VALUE).asString(),
        r.getField(TimeSeriesRecord.CHUNK_START).asLong(),
        r.getField(TimeSeriesRecord.CHUNK_END).asLong(),
        r.getField(TimeSeriesRecord.CHUNK_WINDOW_MS).asLong(),
        r.getField(TimeSeriesRecord.CHUNK_SIZE).asInteger(),
        r.getField(TimeSeriesRecord.CHUNK_FIRST_VALUE).asDouble(),
        r.getField(TimeSeriesRecord.CHUNK_AVG).asDouble(),
        r.getField(TimeSeriesRecord.CHUNK_MIN).asDouble(),
        r.getField(TimeSeriesRecord.CHUNK_MAX).asDouble(),
//        r.getField(TimeSeriesRecord.CHUNK_COUNT).asInteger(),
        r.getField(TimeSeriesRecord.CHUNK_SUM).asDouble(),
        r.getField(TimeSeriesRecord.CHUNK_TREND).asBoolean(),
        r.getField(TimeSeriesRecord.CHUNK_OUTLIER).asBoolean(),
        if (r.hasField(TimeSeriesRecord.CHUNK_SAX)) r.getField(TimeSeriesRecord.CHUNK_SAX).asString() else "",
        r.getField(TimeSeriesRecord.CHUNK_ORIGIN).asString())
      )
      .toDF("id",
        TimeSeriesRecord.CHUNK_YEAR,
        TimeSeriesRecord.CHUNK_MONTH,
        TimeSeriesRecord.CHUNK_DAY,
        TimeSeriesRecord.CODE_INSTALL,
        TimeSeriesRecord.SENSOR,
        TimeSeriesRecord.METRIC_NAME,
        TimeSeriesRecord.CHUNK_VALUE,
        TimeSeriesRecord.CHUNK_START,
        TimeSeriesRecord.CHUNK_END,
        TimeSeriesRecord.CHUNK_WINDOW_MS,
        TimeSeriesRecord.CHUNK_SIZE,
        TimeSeriesRecord.CHUNK_FIRST_VALUE,
        TimeSeriesRecord.CHUNK_AVG,
        TimeSeriesRecord.CHUNK_MIN,
        TimeSeriesRecord.CHUNK_MAX,
//        TimeSeriesRecord.CHUNK_COUNT,
        TimeSeriesRecord.CHUNK_SUM,
        TimeSeriesRecord.CHUNK_TREND,
        TimeSeriesRecord.CHUNK_OUTLIER,
        TimeSeriesRecord.CHUNK_SAX,
        TimeSeriesRecord.CHUNK_ORIGIN)

    savedDF.write
      .format("solr")
      .options(Map(
        "zkhost" -> options.zkHosts,
        "collection" -> options.collectionName
      ))
      .save()

    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.collectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString}")

    savedDF
  }

  private def chunkIntoSeveralTimeSeriesRecord(metricType: String, metricName: String,
                                   values: ArrayDF[String],
                                   starts: ArrayDF[Long],
                                   ends: ArrayDF[Long],
                                   sizes: ArrayDF[Long]) = {
    if (
      values.length != starts.length ||
        values.length != ends.length ||
        values.length != sizes.length
    ) {
      throw new RuntimeException("number of chunk_value, chunk_start, chunk_end and chunk_size are not the same ! This should never happen")
    }
    val numberOfChunkToCompact = values.length
    val numberOfChunkToCompactMinusOne = numberOfChunkToCompact - 1

    val chunks = (for (i <- 0 to numberOfChunkToCompactMinusOne) yield {
      CompactionChunkInfo(values(i), starts(i), ends(i), sizes(i))
    }).flatMap(_.getPoints())
      .sortBy(c => c.getTimestamp)
      .grouped(options.chunkSize)
      .map(chunkPoints => {
        val builder = new MetricTimeSeries.Builder(metricName, metricType)
        chunkPoints.foreach(p => builder.point(p.getTimestamp, p.getValue))
        new TimeSeriesRecord(builder.build())
      })

    if (logger.isTraceEnabled) {
      chunks.foreach(t => {
        logger.trace(s"${metricName} : new chunk size ${t.getChunkSize}, start ${t.getTimeSeries.getStart} - end ${t.getTimeSeries.getEnd}")
      })
    }
    chunks.toList
  }

  private def mergeChunks(sparkSession: SparkSession, solrDf: DataFrame): Dataset[TimeSeriesRecord] = {
    val timeseriesDS = solrDf
      .groupBy(col(TimeSeriesRecord.METRIC_NAME),col(TimeSeriesRecord.CHUNK_YEAR),col(TimeSeriesRecord.CHUNK_MONTH),col(TimeSeriesRecord.CHUNK_DAY))
      .agg(
        sum(col(TimeSeriesRecord.CHUNK_SIZE)).as(point_in_day),
        f.collect_list(col(TimeSeriesRecord.CHUNK_START)).as("starts"),
        f.collect_list(col(TimeSeriesRecord.CHUNK_VALUE)).as("values"),
        f.collect_list(col(TimeSeriesRecord.CHUNK_END)).as("ends"),
        f.collect_list(col(TimeSeriesRecord.CHUNK_SIZE)).as("sizes"),
        f.first(col(TimeSeriesRecord.METRIC_NAME)).as("name")
      )

    import timeseriesDS.sparkSession.implicits._

    timeseriesDS
      .rdd
      .flatMap(mergeChunksIntoSeveralChunk)
      .map(calculMetrics)
      .toDS()
  }

  private def calculMetrics(timeSerie: TimeSeriesRecord): TimeSeriesRecord = {
    // Init the Timeserie processor
    val tsProcessor = new TimeseriesConverter()
    val context = new HistorianContext(tsProcessor)
    context.setProperty(TimeseriesConverter.GROUPBY.getName, TimeSeriesRecord.METRIC_NAME)
    context.setProperty(TimeseriesConverter.METRIC.getName,
      s"first;min;max;count;sum;avg;count;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")
    tsProcessor.init(context)
    tsProcessor.computeValue(timeSerie)
    tsProcessor.computeMetrics(timeSerie)
    EvoaUtils.setBusinessFields(timeSerie)
    EvoaUtils.setDateFields(timeSerie)
    EvoaUtils.setHashId(timeSerie)
    EvoaUtils.setChunkOrigin(timeSerie, TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR)
    timeSerie
  }

  private def mergeChunksIntoSeveralChunk(r: Row): List[TimeSeriesRecord] = {
    val name = r.getAs[String]("name")
    val values = r.getAs[ArrayDF[String]]("values")
    val starts = r.getAs[ArrayDF[Long]]("starts")
    val ends = r.getAs[ArrayDF[Long]]("ends")
    val sizes = r.getAs[ArrayDF[Long]]("sizes")
    val totalPoints = r.getAs[Long](point_in_day)
    logger.trace(s"A total of points of $totalPoints")
    val chunked = chunkIntoSeveralTimeSeriesRecord("evoa_measure", name, values, starts, ends, sizes)
    chunked
  }

  /**
   * Compact chunks of historian
   */
  override def run(spark: SparkSession): Unit = {
    val timeseriesDS = loadDataFromSolR(spark)
    val mergedTimeseriesDS = mergeChunks(sparkSession = spark, timeseriesDS)
    saveNewChunksToSolR(mergedTimeseriesDS)
  }
}
