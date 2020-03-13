package com.hurence.historian

import java.util

import com.hurence.logisland.record.{EvoaUtils, TimeSeriesRecord}
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.converter.common.{DoubleList, LongList}
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.lang.ArrayUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.params.MapSolrParams
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ChunkCompactorJobStrategy1(options: ChunkCompactorConf) extends ChunkCompactor {

  private val logger = LoggerFactory.getLogger(classOf[ChunkCompactorJobStrategy1])

  implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord]
  val queryFilter = s"year:${options.year} AND month:${options.month} AND day:${options.day}"

  def this() {
    this(ChunkCompactorJob.defaultConf)
  }

  private def loadDataFromSolR(spark: SparkSession, filterQuery: String): Dataset[TimeSeriesRecord] = {

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName,
      /*   "splits" -> "true",
         "split_field"-> "name",
         "splits_per_shard"-> "50",*/
      "sort" -> "chunk_start asc",
      "fields" -> "name,chunk_value,chunk_start,chunk_end",
      "filters" -> s"chunk_origin:logisland AND year:${options.year} AND month:${options.month} AND day:${options.day} AND $filterQuery"
    )

    logger.info(s"$solrOpts")

    spark.read
      .format("solr")
      .options(solrOpts)
      .load
      .map(r => new TimeSeriesRecord("evoa_measure",
        r.getAs[String]("name"),
        r.getAs[String]("chunk_value"),
        r.getAs[Long]("chunk_start"),
        r.getAs[Long]("chunk_end")))

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

  private def getCodeInstallList() = {
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

  private def getMetricNameList() = {
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

  private def mergeChunks(timeseriesDS: Dataset[TimeSeriesRecord]): Dataset[TimeSeriesRecord] = {

    import timeseriesDS.sparkSession.implicits._

    def merge(chunk1: TimeSeriesRecord, chunk2: TimeSeriesRecord): TimeSeriesRecord = {


      /**
       * Initialize all data structures
       */
      val series1 = chunk1.getTimeSeries.points().iterator().asScala.toList
      val series2 = chunk2.getTimeSeries.points().iterator().asScala.toList

      val tsBuilder = new MetricTimeSeries.Builder(chunk1.getMetricName, chunk1.getType)
        .attributes(chunk1.getTimeSeries.attributes())

      /**
       * loop around the points to be merged
       */
      // first part of the merge
      var i, j, numAddedPoints = 0

      def addPointFromSerie2 = {
        numAddedPoints += 1
        tsBuilder.point(series2(j).getTimestamp, series2(j).getValue)
        j += 1
      }

      def addPointFromSerie1 = {
        numAddedPoints += 1
        tsBuilder.point(series1(i).getTimestamp, series1(i).getValue)
        i += 1
      }

      while (i < series1.size && j < series2.size) {
        if (series1(i).getTimestamp == series2(j).getTimestamp) {
          addPointFromSerie2
          addPointFromSerie1
        } else if (series1(i).getTimestamp < series2(j).getTimestamp) {
          addPointFromSerie1
        } else {
          addPointFromSerie2
        }
      }

      // second part with the remaining since one of the list is now empty
      if (i == series1.size) {
        while (j < series2.size) {
          addPointFromSerie2
        }
      } else {
        while (i < series1.size) {
          addPointFromSerie1
        }
      }


      /**
       * now we're done and we can build our
       */
      val tsRecord = new TimeSeriesRecord(tsBuilder.build())

      tsRecord
    }

    timeseriesDS
      .rdd
      .map( r => (r.getMetricName, r))
      .reduceByKey((chunk1, chunk2) => merge(chunk1, chunk2))
      .mapPartitions(p => {

        if (p.nonEmpty) {
          // Init the Timeserie processor
          val tsProcessor = new TimeseriesConverter()
          val context = new HistorianContext(tsProcessor)
          context.setProperty(TimeseriesConverter.GROUPBY.getName, TimeSeriesRecord.METRIC_NAME)
          context.setProperty(TimeseriesConverter.METRIC.getName,
            s"first;min;max;count;sum;avg;count;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")

          tsProcessor.init(context)

          p.flatMap(mergedRecord => {

            val splittedRecords = new ListBuffer[TimeSeriesRecord]()
            if (mergedRecord._2.getChunkSize <= options.chunkSize) {
              splittedRecords += mergedRecord._2
            } else {

              val timestamps = mergedRecord._2.getTimeSeries.getTimestamps.toArray
              val values = mergedRecord._2.getTimeSeries.getValues.toArray
              val numChunks = 1 + (timestamps.length / options.chunkSize)

              for (a <- 0 until numChunks) {
                val chunkTimestamps = new LongList(options.chunkSize)
                chunkTimestamps.addAll(ArrayUtils.subarray(timestamps, a * options.chunkSize, (a + 1) * options.chunkSize))

                val chunkValues = new DoubleList(options.chunkSize)
                chunkValues.addAll(ArrayUtils.subarray(values, a * options.chunkSize, (a + 1) * options.chunkSize))

                val timeseries = new MetricTimeSeries.Builder(mergedRecord._2.getMetricName, mergedRecord._2.getType)
                  .attributes(mergedRecord._2.getTimeSeries.attributes())
                  .points(chunkTimestamps, chunkValues)
                  .build()

                val tsRecord = new TimeSeriesRecord(timeseries)
                logger.info(s"${mergedRecord._2.getMetricName} ($a/$numChunks) : new record size ${tsRecord.getChunkSize}, start ${tsRecord.getTimeSeries.getStart} - end ${tsRecord.getTimeSeries.getEnd} ")
                splittedRecords += tsRecord
              }
            }

            splittedRecords.foreach(record => {
              tsProcessor.computeValue(record)
              tsProcessor.computeMetrics(record)
              EvoaUtils.setBusinessFields(record)
              EvoaUtils.setDateFields(record)
              EvoaUtils.setHashId(record)
              EvoaUtils.setChunkOrigin(record, TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR)
            })

            splittedRecords
          })
        } else
          Iterator.empty

      }).toDS()


  }

  /**
   * Compact chunks of historian
   */
  override def run(spark: SparkSession): Unit = {
      getMetricNameList()
        .foreach(name => {
          val timeseriesDS = loadDataFromSolR(spark, s"name:$name")
          val mergedTimeseriesDS = mergeChunks(timeseriesDS)
          val savedDS = saveNewChunksToSolR(mergedTimeseriesDS)
          timeseriesDS.unpersist()
          mergedTimeseriesDS.unpersist()
          savedDS.unpersist()
        })
  }
}
