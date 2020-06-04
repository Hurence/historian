package com.hurence.historian.spark.compactor

import com.hurence.historian.spark.compactor.job.CompactorJobReport
import com.hurence.logisland.record.TimeSeriesRecord
import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object ChunkCompactorJob extends Serializable {

  private val logger = LoggerFactory.getLogger(ChunkCompactorJob.getClass.getCanonicalName)

  val DEFAULT_CHUNK_SIZE = 1440
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 100

  case class ChunkCompactorJobOptions(master: String,
                                      appName: String,
                                      useKerberos: Boolean,
                                      zkHosts: String,
                                      collectionName: String,
                                      chunkSize: Int,
                                      saxAlphabetSize: Int,
                                      saxStringLength: Int,
                                      taggingChunksToCompact: Boolean,
                                      useCache: Boolean)

  val defaultJobOptions = ChunkCompactorJobOptions("local[*]", "", false, "zookeeper:2181", "historian", 1440, 7, 100, true, true)

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

    createCompactor(options).run(spark)
    // TODO remove old logisland chunks
    spark.close()
  }



  private def createCompactor(jobConf: ChunkCompactorJobOptions): ChunkCompactor = {
    val conf = buildCompactorConf2(jobConf)
    new ChunkCompactorJobStrategy2SchemaVersion0(conf)
  }


  def buildCompactorConf2(jobConf: ChunkCompactorJobOptions): ChunkCompactorConfStrategy2 = {
    ChunkCompactorConfStrategy2(
      jobConf.zkHosts,
      jobConf.collectionName,
      CompactorJobReport.DEFAULT_COLLECTION,
      jobConf.chunkSize,
      jobConf.saxAlphabetSize,
      jobConf.saxStringLength,
      s"${TimeSeriesRecord.CHUNK_ORIGIN}:logisland",
      jobConf.taggingChunksToCompact,
      jobConf.useCache
    )
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

    options.addOption(Option.builder("nt")
      .longOpt("no-tagging-chunks")
      .hasArg(false)
      .optionalArg(true)
      .desc("if set, the compactor will not tag chunks to compact. Therefore when deleting chunks at end " +
        "there is a risk that to delete unchunked data (to lost data). Depending on if the query match different chunks at " +
        "the start than at the end of the job.")
      .build()
    )

    options.addOption(Option.builder("c")
      .longOpt("use-cache")
      .hasArg(false)
      .optionalArg(true)
      .desc("use cache on loaded chunks dataset. This may improve or decrease performance depending on " +
        " memory capacity of the job and the size of the dataset.")
      .build()
    )
    // parse the command line arguments
    val line = parser.parse(options, args)
    val sparkMaster: String = if (line.hasOption("ms")) line.getOptionValue("ms") else "local[*]"
    val useKerberos: Boolean = if (line.hasOption("kb")) true else false
    val zkHosts: String = if (line.hasOption("zk")) line.getOptionValue("zk") else "localhost:2181"
    val collectionName = if (line.hasOption("col")) line.getOptionValue("col") else "historian"
    val chunksSize = if (line.hasOption("cs")) line.getOptionValue("chunks").toInt else ChunkCompactorJob.DEFAULT_CHUNK_SIZE
    val alphabetSize = if (line.hasOption("sas")) line.getOptionValue("sa").toInt else ChunkCompactorJob.DEFAULT_SAX_ALPHABET_SIZE
    val saxStringLength = if (line.hasOption("ssl")) line.getOptionValue("sl").toInt else ChunkCompactorJob.DEFAULT_SAX_STRING_LENGTH
    val taggingChunksToCompact: Boolean = !line.hasOption("nt")
    val useCache: Boolean = line.hasOption("c")

    // build the option handler
    val opts = ChunkCompactorJobOptions(sparkMaster,
      "ChunkCompactor",
      useKerberos,
      zkHosts,
      collectionName,
      chunksSize,
      alphabetSize,
      saxStringLength,
      taggingChunksToCompact,
      useCache
    )
    logger.info(s"Command line options : $opts")
    opts
  }

}
