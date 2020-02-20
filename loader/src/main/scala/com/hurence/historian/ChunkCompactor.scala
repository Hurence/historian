package com.hurence.historian


import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date}

import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.record.{EvoaUtils, FieldType, TimeSeriesRecord}
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.converter.common.{DoubleList, LongList}
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.commons.lang.ArrayUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import java.util.stream.Collectors

import org.apache.solr.common.params.MapSolrParams

class ChunkCompactor extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[ChunkCompactor])

  val DEFAULT_CHUNK_SIZE = 1440
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 100


  implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord]

  case class ChunkCompactorOptions(master: String,
                                   zkHosts: String,
                                   collectionName: String,
                                   appName: String,
                                   chunkSize: Int,
                                   saxAlphabetSize: Int,
                                   saxStringLength: Int,
                                   useKerberos: Boolean,
                                   year: Int,
                                   month: Int,
                                   day: Int)

  val options = ChunkCompactorOptions("local[*]", "zookeeper:2181", "historian", "", 1440, 7, 100, false, 2019, 6, 19)

  def parseCommandLine(args: Array[String]): ChunkCompactorOptions = {

    val parser = new DefaultParser
    val options = new Options


    val helpMsg = "Print this message."
    val help = new Option("help", helpMsg)
    options.addOption(help)

    options.addOption(
      Option.builder("ms")
        .longOpt("spark-master")
        .hasArg(true)
        .desc("spark master")
        .build()
    )

    options.addOption(Option.builder("zk")
      .longOpt("zookeeper-quorum")
      .hasArg(true)
      .desc(s"the zookeeper quorum for solr collection")
      .build()
    )
    options.addOption(Option.builder("col")
      .longOpt("collection-name")
      .hasArg(true)
      .desc(s"Solr collection name, default historian")
      .build()
    )

    options.addOption(Option.builder("cs")
      .longOpt("chunks-size")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"num points in a chunk, default $DEFAULT_CHUNK_SIZE")
      .build()
    )

    options.addOption(Option.builder("sas")
      .longOpt("sax-alphabet-size")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"size of alphabet, default $DEFAULT_SAX_ALPHABET_SIZE")
      .build()
    )

    options.addOption(Option.builder("ssl")
      .longOpt("sax-string-length")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"num points in a chunk, default $DEFAULT_SAX_STRING_LENGTH")
      .build()
    )

    options.addOption(Option.builder("kb")
      .longOpt("kerberos")
      .optionalArg(true)
      .desc("do we use kerberos ?, default false")
      .build()
    )

    options.addOption(Option.builder("date")
      .longOpt("recompaction-date")
      .hasArg(true)
      .optionalArg(true)
      .desc("the day date to recompact in the form of yyyy-MM-dd")
      .build()
    )

    // parse the command line arguments
    val line = parser.parse(options, args)
    val sparkMaster = if (line.hasOption("ms")) line.getOptionValue("master") else "local[*]"
    val useKerberos = if (line.hasOption("kb")) true else false
    val zkHosts = if (line.hasOption("zk")) line.getOptionValue("zk") else "localhost:2181"
    val collectionName = if (line.hasOption("col")) line.getOptionValue("col") else "historian"
    val chunksSize = if (line.hasOption("cs")) line.getOptionValue("chunks").toInt else DEFAULT_CHUNK_SIZE
    val alphabetSize = if (line.hasOption("sas")) line.getOptionValue("sa").toInt else DEFAULT_SAX_ALPHABET_SIZE
    val saxStringLength = if (line.hasOption("ssl")) line.getOptionValue("sl").toInt else DEFAULT_SAX_STRING_LENGTH
    val dateTokens = if (line.hasOption("date")) {
      line.getOptionValue("date").split("-")
    } else {
      val DATE_FORMAT = "yyyy-MM-dd"
      val dateFormat = new SimpleDateFormat(DATE_FORMAT)
      dateFormat.format(new Date())
        .split("-")
    }

    // build the option handler
    val opts = ChunkCompactorOptions(sparkMaster,
      zkHosts,
      collectionName,
      "ChunkCompactor",
      chunksSize,
      alphabetSize,
      saxStringLength,
      useKerberos,
      dateTokens(0).toInt,
      dateTokens(1).toInt,
      dateTokens(2).toInt)


    logger.info(s"Command line options : $opts")
    opts
  }


  def loadDataFromSolR(spark: SparkSession, options: ChunkCompactorOptions, codeInstall:String): Dataset[TimeSeriesRecord] = {

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName,
      "fields" -> "id,name,chunk_value,chunk_start,chunk_end",
      "filters" -> s"chunk_origin:logisland AND year:${options.year} AND month:${options.month} AND day:${options.day} AND code_install:$codeInstall"
    )

    logger.info(s"$solrOpts")

    spark.read
      .format("solr")
      .options(solrOpts)
      .load
      .map(r => new TimeSeriesRecord("evoa_measure",
        r.getAs[String]("id"),
        r.getAs[String]("name"),
        r.getAs[String]("chunk_value"),
        r.getAs[Long]("chunk_start"),
        r.getAs[Long]("chunk_end")))

  }

  def saveNewChunksToSolR(timeseriesDS: Dataset[TimeSeriesRecord], options: ChunkCompactorOptions) = {


    import timeseriesDS.sparkSession.implicits._

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName
    )

    logger.info(s"start saving new chunks to ${options.collectionName}")
    val savedDF = timeseriesDS
      .map(r => (
        r.getId,
        r.getField("year").asInteger(),
        r.getField("month").asInteger(),
        r.getField("day").asInteger(),
        r.getField("code_install").asString(),
        r.getField("sensor").asString(),
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
        r.getField(TimeSeriesRecord.CHUNK_COUNT).asInteger(),
        r.getField(TimeSeriesRecord.CHUNK_SUM).asDouble(),
        r.getField(TimeSeriesRecord.CHUNK_TREND).asBoolean(),
        r.getField(TimeSeriesRecord.CHUNK_OUTLIER).asBoolean(),
        if (r.hasField(TimeSeriesRecord.CHUNK_SAX)) r.getField(TimeSeriesRecord.CHUNK_SAX).asString() else "",
        r.getField(TimeSeriesRecord.CHUNK_ORIGIN).asString())
      )
      .toDF("id",
        "year",
        "month",
        "day",
        "code_install",
        "sensor",
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
        TimeSeriesRecord.CHUNK_COUNT,
        TimeSeriesRecord.CHUNK_SUM,
        TimeSeriesRecord.CHUNK_TREND,
        TimeSeriesRecord.CHUNK_OUTLIER,
        TimeSeriesRecord.CHUNK_SAX,
        TimeSeriesRecord.CHUNK_ORIGIN)


    savedDF.write
      .format("solr")
      .options(solrOpts)
      .save()

    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.collectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString}")

    savedDF
  }

  def getCodeInstallList(queryFilter: String,options: ChunkCompactorOptions) = {
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
    queryParamMap.put("facet","on")
    queryParamMap.put("facet.field","code_install")
    queryParamMap.put("facet.limit","-1")
    queryParamMap.put("facet.mincount","1")

    val queryParams = new MapSolrParams(queryParamMap)

    val result = solrCloudClient.query(options.collectionName, queryParams)
    val facetResult = result.getFacetField("code_install")

    logger.info(result.toString)
    logger.info(facetResult.toString)

    facetResult.getValues.asScala.map( r => r.getName).toList
  }

  def removeChunksFromSolR(queryFilter: String, options: ChunkCompactorOptions) = {


    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)

    val query = s"chunk_origin:${TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR} AND $queryFilter"

    // Explicit commit to make sure all docs are visible
    logger.info(s"will permantly delete docs matching $query from ${options.collectionName}}")
    solrCloudClient.deleteByQuery(options.collectionName, query)
    solrCloudClient.commit(options.collectionName, true, true)
  }


  def mergeChunks(timeseriesDS: Dataset[TimeSeriesRecord], options: ChunkCompactorOptions): Dataset[TimeSeriesRecord] = {

    import timeseriesDS.sparkSession.implicits._

    def merge(g1: TimeSeriesRecord, g2: TimeSeriesRecord): TimeSeriesRecord = {


      /**
        * Initialize all data structures
        */
      val series1 = g1.getTimeSeries.points().iterator().asScala.toList
      val series2 = g2.getTimeSeries.points().iterator().asScala.toList

      val tsBuilder = new MetricTimeSeries.Builder(g1.getMetricName, g1.getType)
        .attributes(g1.getTimeSeries.attributes())

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
      .groupByKey(_.getMetricName)
      .reduceGroups((g1, g2) => merge(g1, g2))
      .map(r => r._2)
      .repartition(10)
      .mapPartitions(p => {
        // Init the Timeserie processor
        val tsProcessor = new TimeseriesConverter()
        val context = new StandardProcessContext(tsProcessor, "")
        context.setProperty(TimeseriesConverter.GROUPBY.getName, TimeSeriesRecord.METRIC_NAME)
        context.setProperty(TimeseriesConverter.METRIC.getName,
          s"first;min;max;count;sum;avg;count;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")

        tsProcessor.init(context)

        p.flatMap(mergedRecord => {

          val splittedRecords = new ListBuffer[TimeSeriesRecord]()
          if (mergedRecord.getChunkSize <= options.chunkSize) {
            splittedRecords += mergedRecord
          } else {

            val timestamps = mergedRecord.getTimeSeries.getTimestamps.toArray
            val values = mergedRecord.getTimeSeries.getValues.toArray
            val numChunks = 1 + (timestamps.length / options.chunkSize)

            for (a <- 0 until numChunks) {
              val chunkTimestamps = new LongList(options.chunkSize)
              chunkTimestamps.addAll(ArrayUtils.subarray(timestamps, a * options.chunkSize, (a + 1) * options.chunkSize))

              val chunkValues = new DoubleList(options.chunkSize)
              chunkValues.addAll(ArrayUtils.subarray(values, a * options.chunkSize, (a + 1) * options.chunkSize))

              val timeseries = new MetricTimeSeries.Builder(mergedRecord.getMetricName, mergedRecord.getType)
                .attributes(mergedRecord.getTimeSeries.attributes())
                .points(chunkTimestamps, chunkValues)
                .build()

              val tsRecord = new TimeSeriesRecord(timeseries)
              logger.info(s"${mergedRecord.getMetricName} ($a/$numChunks) : new record size ${tsRecord.getChunkSize}, start ${tsRecord.getTimeSeries.getStart} - end ${tsRecord.getTimeSeries.getEnd} ")
              splittedRecords += tsRecord
            }
          }

          splittedRecords.foreach(record => {
            tsProcessor.computeValue(record)
            tsProcessor.computeMetrics(record)
            EvoaUtils.setChunkOrigin(record, TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR)
            EvoaUtils.setBusinessFields(record)
            EvoaUtils.setDateFields(record)
            EvoaUtils.setHashId(record)
          })

          splittedRecords
        })
      })


  }

}

object ChunkCompactor {
  /**
    *
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val compactor = new ChunkCompactor()

    // get arguments
    val options = compactor.parseCommandLine(args)

    // setup spark session
    val spark = SparkSession.builder
      .appName(options.appName)
      .master(options.master)
      .getOrCreate()


    val queryFilter = s"year:${options.year} AND month:${options.month} AND day:${options.day}"


    // remove previous compacted chunks from the same day
    compactor.removeChunksFromSolR(queryFilter, options)


    compactor.getCodeInstallList(queryFilter, options)
      .foreach( codeInstall => {


      // load raw chunks
      val timeseriesDS = compactor.loadDataFromSolR(spark, options, codeInstall)

      // merge those chunks for the given day
      val mergedTimeseriesDS = compactor.mergeChunks(timeseriesDS, options)

      val savedDS = compactor.saveNewChunksToSolR(mergedTimeseriesDS, options)
      /*
          compactor.removeUselessChunksFromSolR(timeseriesDS, options)*/
    })


    spark.close()
  }

}
