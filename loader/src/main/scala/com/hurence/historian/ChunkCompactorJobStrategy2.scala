package com.hurence.historian

import java.util

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import com.hurence.logisland.record.{EvoaUtils, Point, TimeSeriesRecord}
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.converter.common.{DoubleList, LongList}
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.lang.ArrayUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.MapSolrParams
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => f}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{WrappedArray => ArrayDF}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ChunkCompactorJobStrategy2(options: ChunkCompactorConf) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[ChunkCompactorJobStrategy2])

  implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord]
  val queryFilter = s"year:${options.year} AND month:${options.month} AND day:${options.day}"

  def this() {
    this(ChunkCompactorJob.defaultConf)
  }

  def loadDataFromSolR(spark: SparkSession, filterQuery: String): DataFrame = {

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName,
   /*   "splits" -> "true",
      "split_field"-> "name",
      "splits_per_shard"-> "50",*/
      "sort" -> "chunk_start asc",
      "fields" -> "name,chunk_value,chunk_start,chunk_end,chunk_size,year,month,day",
      "filters" -> s"chunk_origin:logisland AND year:${options.year} AND month:${options.month} AND day:${options.day} AND $filterQuery"
    )

    logger.info(s"$solrOpts")

    spark.read
      .format("solr")
      .options(solrOpts)
      .load

  }

  def saveNewChunksToSolR(timeseriesDS: Dataset[TimeSeriesRecord]) = {


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

  def getCodeInstallList() = {
    logger.info(s"first looking for code_install to loop on")
    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)

    val query = new SolrQuery
    query.setRows(0)
    query.setFacet(true)
    query.addFacetField("code_install")
    query.setFacetLimit(-1)
    query.setFacetMinCount(1)
    query.setQuery(null)

    val queryParamMap = new util.HashMap[String, String]()
    queryParamMap.put("q", "*:*")
    queryParamMap.put("fq", queryFilter)
    queryParamMap.put("facet", "on")
    queryParamMap.put("facet.field", "code_install")
    queryParamMap.put("facet.limit", "-1")
    queryParamMap.put("facet.mincount", "1")

    val queryParams = new MapSolrParams(queryParamMap)

    val result = solrCloudClient.query(options.collectionName, queryParams)
    val facetResult = result.getFacetField("code_install")

    logger.info(facetResult.toString)

    facetResult.getValues.asScala.map(r => r.getName).toList
  }

  def getMetricNameList() = {
    logger.info(s"first looking for name to loop on")
    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)

    val query = new SolrQuery
    query.setRows(0)
    query.setFacet(true)
    query.addFacetField("name")
    query.setFacetLimit(-1)
    query.setFacetMinCount(1)
    query.setQuery(null)

    val queryParamMap = new util.HashMap[String, String]()
    queryParamMap.put("q", "*:*")
    queryParamMap.put("fq", queryFilter)
    queryParamMap.put("facet", "on")
    queryParamMap.put("facet.field", "name")
    queryParamMap.put("facet.limit", "-1")
    queryParamMap.put("facet.mincount", "1")

    val queryParams = new MapSolrParams(queryParamMap)

    val result = solrCloudClient.query(options.collectionName, queryParams)
    val facetResult = result.getFacetField("name")

    logger.info(facetResult.toString)

    facetResult.getValues.asScala.map(r => r.getName).toList
  }

  def removeChunksFromSolR() = {


    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)

    val query = s"chunk_origin:${TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR} AND $queryFilter"

    // Explicit commit to make sure all docs are visible
    logger.info(s"will permantly delete docs matching $query from ${options.collectionName}}")
    solrCloudClient.deleteByQuery(options.collectionName, query)
    solrCloudClient.commit(options.collectionName, true, true)
  }


  def chunkTimeSeriesRecord(metricType: String, metricName: String,
                            values: ArrayDF[String],
                            starts: ArrayDF[Long],
                            ends: ArrayDF[Long],
                            sizes: ArrayDF[Long]) = {
    val chunkStart = starts.min
    val chunkEnd = ends.max
    val builder = new MetricTimeSeries.Builder(metricName, metricType)
      .start(chunkStart)
      .end(chunkEnd)

    if (
      values.length != starts.length ||
        values.length != ends.length ||
        values.length != sizes.length
    ) {
      throw new RuntimeException("number of chunk_value, chunk_start, chunk_end and chunk_size are not the same ! This should never happen")
    }
    val numberOfChunkToCompact = values.length
    val numberOfChunkToCompactMinusOne = numberOfChunkToCompact - 1
    val chunkInfos :List[CompactionChunkInfo] = (for(i <- 0 to numberOfChunkToCompactMinusOne) yield {
      CompactionChunkInfo(values(i), starts(i), ends(i), sizes(i))
    }).toList
    val compactor = new ChunkCompactor()
    chunkInfos.foreach(c => compactor.addChunk(c))
    compactor.getPoints().foreach((point: Point) => builder.point(point.getTimestamp, point.getValue))
    val timeSeries: MetricTimeSeries = builder.build
    val chunk = new TimeSeriesRecord(timeSeries)
    chunk
  }

  def mergeChunks(sparkSession: SparkSession, solrDf: DataFrame): Dataset[TimeSeriesRecord] = {
    val point_in_day = "number_points_day"
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
    timeseriesDS
      .sort(col(point_in_day).desc)
      .show(10, false)

    import timeseriesDS.sparkSession.implicits._

    def mergeChunks(r: Row): TimeSeriesRecord = {
      val name = r.getAs[String]("name")
      val values = r.getAs[ArrayDF[String]]("values")
      val starts = r.getAs[ArrayDF[Long]]("starts")
      val ends = r.getAs[ArrayDF[Long]]("ends")
      val sizes = r.getAs[ArrayDF[Long]]("sizes")
      val totalPoints = r.getAs[Long](point_in_day)
      logger.info(s"fake chunking for metric $name, a total of points of $totalPoints")
      val chunked = chunkTimeSeriesRecord("evoa_measure", name, values, starts, ends, sizes)
      chunked
    }

    timeseriesDS
      .rdd
      .map(mergeChunks)
      .toDS()

//    timeseriesDS
//      .mapPartitions(p => {
//        if (p.nonEmpty) {
//          // Init the Timeserie processor
//          val tsProcessor = new TimeseriesConverter()
//          val context = new HistorianContext(tsProcessor)
//          context.setProperty(TimeseriesConverter.GROUPBY.getName, TimeSeriesRecord.METRIC_NAME)
//          context.setProperty(TimeseriesConverter.METRIC.getName,
//            s"first;min;max;count;sum;avg;count;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")
//
//          tsProcessor.init(context)
//
//          p.flatMap(mergedRecord => {
//            val splittedRecords = new ListBuffer[TimeSeriesRecord]()
//            if (mergedRecord._2.getChunkSize <= options.chunkSize) {
//              splittedRecords += mergedRecord._2
//            } else {
//
//              val timestamps = mergedRecord._2.getTimeSeries.getTimestamps.toArray
//              val values = mergedRecord._2.getTimeSeries.getValues.toArray
//              val numChunks = 1 + (timestamps.length / options.chunkSize)
//
//              for (a <- 0 until numChunks) {
//                val chunkTimestamps = new LongList(options.chunkSize)
//                chunkTimestamps.addAll(ArrayUtils.subarray(timestamps, a * options.chunkSize, (a + 1) * options.chunkSize))
//
//                val chunkValues = new DoubleList(options.chunkSize)
//                chunkValues.addAll(ArrayUtils.subarray(values, a * options.chunkSize, (a + 1) * options.chunkSize))
//
//                val timeseries = new MetricTimeSeries.Builder(mergedRecord._2.getMetricName, mergedRecord._2.getType)
//                  .attributes(mergedRecord._2.getTimeSeries.attributes())
//                  .points(chunkTimestamps, chunkValues)
//                  .build()
//
//                val tsRecord = new TimeSeriesRecord(timeseries)
//                logger.info(s"${mergedRecord._2.getMetricName} ($a/$numChunks) : new record size ${tsRecord.getChunkSize}, start ${tsRecord.getTimeSeries.getStart} - end ${tsRecord.getTimeSeries.getEnd} ")
//                splittedRecords += tsRecord
//              }
//            }
//
//            splittedRecords.foreach(record => {
//              tsProcessor.computeValue(record)
//              tsProcessor.computeMetrics(record)
//              EvoaUtils.setBusinessFields(record)
//              EvoaUtils.setDateFields(record)
//              EvoaUtils.setHashId(record)
//              EvoaUtils.setChunkOrigin(record, TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR)
//            })
//
//            splittedRecords
//          })
//        } else
//          Iterator.empty
//      }).toDS()


  }

}
