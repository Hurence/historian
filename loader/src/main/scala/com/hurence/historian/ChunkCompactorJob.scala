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

  case class ChunkCompactorConfStrategy2(zkHosts: String,
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
                                      zkHosts: String,
                                      collectionName: String,
                                      chunkSize: Int,
                                      saxAlphabetSize: Int,
                                      saxStringLength: Int,
                                      year: Int,
                                      month: Int,
                                      day: Int)

  val defaultConf = ChunkCompactorConf("zookeeper:2181", "historian", 1440, 7, 100, 2019, 6, 19)
  val defaultJobOptions = ChunkCompactorJobOptions("local[*]", "", false, "zookeeper:2181", "historian", 1440, 7, 100, 2019, 6, 19)

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

    //Remove logisland chunks of precedent compactor job
    removeChunksFromSolR(options.collectionName, options.zkHosts)
    createCompactor(options).run(spark)
    // TODO remove old logisland chunks
    spark.close()
  }



  private def createCompactor(jobConf: ChunkCompactorJobOptions): ChunkCompactor = {
//    val conf = buildCompactorConf(jobConf)
//    new ChunkCompactorJobStrategy1(conf)
    val conf = buildCompactorConf2(jobConf)
    new ChunkCompactorJobStrategy2(conf)
  }

  def buildCompactorConf(jobConf: ChunkCompactorJobOptions): ChunkCompactorConf = {
      ChunkCompactorConf(
        jobConf.zkHosts,
        jobConf.collectionName,
        jobConf.chunkSize,
        jobConf.saxAlphabetSize,
        jobConf.saxStringLength,
        jobConf.year,
        jobConf.month,
        jobConf.day
      )
  }

  def buildCompactorConf2(jobConf: ChunkCompactorJobOptions): ChunkCompactorConfStrategy2 = {
    ChunkCompactorConfStrategy2(
      jobConf.zkHosts,
      jobConf.collectionName,
      jobConf.chunkSize,
      jobConf.saxAlphabetSize,
      jobConf.saxStringLength,
      jobConf.year,
      jobConf.month,
      jobConf.day
    )
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
      zkHosts,
      collectionName,
      chunksSize,
      alphabetSize,
      saxStringLength,
      dateTokens(0).toInt,
      dateTokens(1).toInt,
      dateTokens(2).toInt
    )
    logger.info(s"Command line options : $opts")
    opts
  }

}