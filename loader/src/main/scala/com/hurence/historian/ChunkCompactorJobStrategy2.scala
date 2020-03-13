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
  private val totalNumberOfPointColumnName = "number_points_day"
  private val startsColumnName = "starts"
  private val valuesColumnName = "values"
  private val endsColumnName = "ends"
  private val sizesColumnName = "sizes"
  private val nameColumnName = "name"

  private implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord]

  private def loadDataFromSolR(spark: SparkSession): DataFrame = {
    val fields = s"${TimeSeriesRecord.METRIC_NAME},${TimeSeriesRecord.CHUNK_VALUE},${TimeSeriesRecord.CHUNK_START}," +
      s"${TimeSeriesRecord.CHUNK_END},${TimeSeriesRecord.CHUNK_SIZE},${TimeSeriesRecord.CHUNK_YEAR}," +
      s"${TimeSeriesRecord.CHUNK_MONTH},${TimeSeriesRecord.CHUNK_DAY}"

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName,
   /*   "splits" -> "true",
      "split_field"-> "name",
      "splits_per_shard"-> "50",*/
      "sort" -> "chunk_start asc",
      "fields" -> fields,
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

  /**
   * determine grouping criteria depending on number of points
   * If 10 per day, 300 per month, 3000 per year then group by year for those metrics
   * If 10000 per day then group by day for those metrics
   *
   * @param sparkSession
   * @param solrDf
   * @return
   */
  private def mergeChunks(sparkSession: SparkSession, solrDf: DataFrame): Dataset[TimeSeriesRecord] = {
    val maxNumberOfPointInPartition = 100000L //TODO this means we can not compact chunks of more than 100000
//    solrDf.cache()
    val dailyChunks: Dataset[TimeSeriesRecord] = chunkDailyMetrics(solrDf, maxNumberOfPointInPartition)
//    val monthlyChunks: Dataset[TimeSeriesRecord] = chunkMonthlyMetrics(solrDf, maxNumberOfPointInPartition)
//    val yearlyChunks: Dataset[TimeSeriesRecord] = chunkYearlyMetrics(solrDf, maxNumberOfPointInPartition)

//    dailyChunks
//      .union(monthlyChunks)
//      .union(yearlyChunks)
    dailyChunks

//    val monthlyMetrics: Dataset[String] = findMonthlyMetrics(maxNumberOfPointInPartition)
//    val yearlyMetrics: Dataset[String] = findYearlyMetrics(maxNumberOfPointInPartition)
  }

  private def chunkDailyMetrics(solrDf: DataFrame, maxNumberOfPointInPartition: Long): Dataset[TimeSeriesRecord] = {
    val daylyMetrics: Dataset[String] = findDaylyMetrics(solrDf, maxNumberOfPointInPartition)

    val groupedChunkDf = solrDf
      .groupBy(
        col(TimeSeriesRecord.METRIC_NAME),
        col(TimeSeriesRecord.CHUNK_YEAR),
        col(TimeSeriesRecord.CHUNK_MONTH),
        col(TimeSeriesRecord.CHUNK_DAY))
      .agg(
        sum(col(TimeSeriesRecord.CHUNK_SIZE)).as(totalNumberOfPointColumnName),
        f.collect_list(col(TimeSeriesRecord.CHUNK_START)).as(startsColumnName),
        f.collect_list(col(TimeSeriesRecord.CHUNK_VALUE)).as(valuesColumnName),
        f.collect_list(col(TimeSeriesRecord.CHUNK_END)).as(endsColumnName),
        f.collect_list(col(TimeSeriesRecord.CHUNK_SIZE)).as(sizesColumnName),
        f.first(col(TimeSeriesRecord.METRIC_NAME)).as(nameColumnName)
      )

    import groupedChunkDf.sparkSession.implicits._

    groupedChunkDf
      .rdd
      .flatMap(mergeChunksIntoSeveralChunk)
      .map(calculMetrics)
      .toDS()
  }

  def findDaylyMetrics(solrDf: DataFrame, maxNumberOfPointInPartition: Long): Dataset[String] = {
    null//TODO
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
    val name = r.getAs[String](nameColumnName)
    val values = r.getAs[ArrayDF[String]](valuesColumnName)
    val starts = r.getAs[ArrayDF[Long]](startsColumnName)
    val ends = r.getAs[ArrayDF[Long]](endsColumnName)
    val sizes = r.getAs[ArrayDF[Long]](sizesColumnName)
    val totalPoints = r.getAs[Long](totalNumberOfPointColumnName)
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
