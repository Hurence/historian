package com.hurence.historian


import java.text.SimpleDateFormat
import java.util.Date

import com.hurence.logisland.record.TimeSeriesRecord
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object ChunkCompactorJob extends Serializable {

  private val logger = LoggerFactory.getLogger(ChunkCompactorJob.getClass.getCanonicalName)

  val DEFAULT_CHUNK_SIZE = 1440
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 100

  case class ChunkCompactorConf(zkHosts: String,
                                collectionName: String,
                                chunkSize: Int,
                                saxAlphabetSize: Int,
                                saxStringLength: Int,
                                year: Int,
                                month: Int,
                                day: Int)

  case class ChunkCompactorJobOptions(master: String,
                                      appName: String,
                                      useKerberos: Boolean,
                                      compactorConf: ChunkCompactorConf)

  val defaultConf = ChunkCompactorConf("zookeeper:2181", "historian", 1440, 7, 100, 2019, 6, 19)
  val defaultJobOptions = ChunkCompactorJobOptions("local[*]", "", false, defaultConf)

  /**
   *
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // get arguments
    val options = parseCommandLine(args)

    // setup spark session
    val spark = SparkSession.builder
      .appName(options.appName)
      .master(options.master)
      .getOrCreate()

    //Remove logisland chunks of rpecedent compactor job
    removeChunksFromSolR(options.compactorConf.collectionName, options.compactorConf.zkHosts)
//    doStrategy1(options, spark)
    doStrategy2(options, spark)
    // TODO remove old logisland chunks
    spark.close()
  }

  def removeChunksFromSolR(collectionName: String, zkHost: String) = {
    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)

    val query = s"chunk_origin:${TimeSeriesRecord.CHUNK_ORIGIN_COMPACTOR}"

    // Explicit commit to make sure all docs are visible
    logger.info(s"will permantly delete docs matching $query from ${collectionName}}")
    solrCloudClient.deleteByQuery(collectionName, query)
    solrCloudClient.commit(collectionName, true, true)
  }

  private def doStrategy1(options: ChunkCompactorJobOptions, spark: SparkSession) = {
    val compactor = new ChunkCompactorJobStrategy1(options.compactorConf)

    compactor.getMetricNameList()

      .foreach(name => {
        val timeseriesDS = compactor.loadDataFromSolR(spark, s"name:$name")
        val mergedTimeseriesDS = compactor.mergeChunks(timeseriesDS)
        val savedDS = compactor.saveNewChunksToSolR(mergedTimeseriesDS)
        timeseriesDS.unpersist()
        mergedTimeseriesDS.unpersist()
        savedDS.unpersist()
      })
  }

  private def doStrategy2(options: ChunkCompactorJobOptions, spark: SparkSession) = {
    val compactor = new ChunkCompactorJobStrategy2(options.compactorConf)
    val timeseriesDS = compactor.loadDataFromSolR(spark, s"name:*")
    val mergedTimeseriesDS = compactor.mergeChunks(sparkSession = spark, timeseriesDS)
    compactor.saveNewChunksToSolR(mergedTimeseriesDS)
  }

  def parseCommandLine(args: Array[String]): ChunkCompactorJobOptions = {

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
      .desc(s"num points in a chunk, default ${ChunkCompactorJob.DEFAULT_CHUNK_SIZE}")
      .build()
    )

    options.addOption(Option.builder("sas")
      .longOpt("sax-alphabet-size")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"size of alphabet, default ${ChunkCompactorJob.DEFAULT_SAX_ALPHABET_SIZE}")
      .build()
    )

    options.addOption(Option.builder("ssl")
      .longOpt("sax-string-length")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"num points in a chunk, default ${ChunkCompactorJob.DEFAULT_SAX_STRING_LENGTH}")
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
    val sparkMaster = if (line.hasOption("ms")) line.getOptionValue("ms") else "local[*]"
    val useKerberos = if (line.hasOption("kb")) true else false
    val zkHosts = if (line.hasOption("zk")) line.getOptionValue("zk") else "localhost:2181"
    val collectionName = if (line.hasOption("col")) line.getOptionValue("col") else "historian"
    val chunksSize = if (line.hasOption("cs")) line.getOptionValue("chunks").toInt else ChunkCompactorJob.DEFAULT_CHUNK_SIZE
    val alphabetSize = if (line.hasOption("sas")) line.getOptionValue("sa").toInt else ChunkCompactorJob.DEFAULT_SAX_ALPHABET_SIZE
    val saxStringLength = if (line.hasOption("ssl")) line.getOptionValue("sl").toInt else ChunkCompactorJob.DEFAULT_SAX_STRING_LENGTH
    val dateTokens = if (line.hasOption("date")) {
      line.getOptionValue("date").split("-")
    } else {
      val DATE_FORMAT = "yyyy-MM-dd"
      val dateFormat = new SimpleDateFormat(DATE_FORMAT)
      dateFormat.format(new Date())
        .split("-")
    }

    // build the option handler
    val opts = ChunkCompactorJobOptions(sparkMaster,
      "ChunkCompactor",
      useKerberos,
      ChunkCompactorConf(
        zkHosts,
        collectionName,
        chunksSize,
        alphabetSize,
        saxStringLength,
        dateTokens(0).toInt,
        dateTokens(1).toInt,
        dateTokens(2).toInt
      )
    )


    logger.info(s"Command line options : $opts")
    opts
  }

}