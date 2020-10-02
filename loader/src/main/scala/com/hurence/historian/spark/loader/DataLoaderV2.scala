package com.hurence.historian.spark.loader

import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader.{MeasuresReaderType, ReaderFactory}
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.hurence.timeseries.model.Chunk
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.LoggerFactory




class DataLoaderV2 extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[DataLoaderV2])



}

object DataLoaderV2 {


  val DEFAULT_CHUNK_SIZE = 1440
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 100


  case class DataLoaderOptions(master: String,
                               zkHosts: String,
                               collectionName: String,
                               csvFilePath: scala.Option[String],
                               parquetFilePath: scala.Option[String],
                               chunkSize: Int,
                               saxAlphabetSize: Int,
                               saxStringLength: Int,
                               useKerberos: Boolean)


  def parseCommandLine(args: Array[String]): DataLoaderOptions = {

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

    options.addOption(Option.builder("csv")
      .longOpt("csv-file-path")
      .hasArg(true)
      .desc(s"File path mask, can be anything like /a/B/c/*/*pr*/*.csv")
      .build()
    )

    options.addOption(Option.builder("pq")
      .longOpt("parquet-file-path")
      .hasArg(true)
      .desc(s"File path mask, can be anything like /a/B/c/*/*pr*/*.parquet")
      .build()
    )

    options.addOption(Option.builder("cs")
      .longOpt("chunks-size")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"num measures in a chunk, default $DEFAULT_CHUNK_SIZE")
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
      .desc(s"num measures in a chunk, default $DEFAULT_SAX_STRING_LENGTH")
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
    val csvFilePath = if (line.hasOption("csv")) Some(line.getOptionValue("csv")) else None
    val parquetFilePath = if (line.hasOption("pq")) Some(line.getOptionValue("pq")) else None
    val chunksSize = if (line.hasOption("cs")) line.getOptionValue("chunks").toInt else DEFAULT_CHUNK_SIZE
    val alphabetSize = if (line.hasOption("sas")) line.getOptionValue("sa").toInt else DEFAULT_SAX_ALPHABET_SIZE
    val saxStringLength = if (line.hasOption("ssl")) line.getOptionValue("sl").toInt else DEFAULT_SAX_STRING_LENGTH

    // build the option handler
    val opts = DataLoaderOptions(sparkMaster,
      zkHosts,
      collectionName,
      csvFilePath,
      parquetFilePath,
      chunksSize,
      alphabetSize,
      saxStringLength,
      useKerberos)

    logger.info(s"Command line options : $opts")
    opts
  }

  private val logger = LoggerFactory.getLogger(classOf[DataLoaderV2])

  /**
    *
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // get arguments
   // val options = dataLoader.parseCommandLine(args)
    val options = DataLoaderOptions("local[*]", "zookeeper:9983", "historian", Some("/Users/tom/Documents/workspace/historian/loader/src/test/resources/it-data-4metrics.csv.gz"), None, 1440, 7, 20, useKerberos = false)

    // setup spark session
    val spark = SparkSession.builder
      .appName("DataLoaderV2")
      .master(options.master)
      .getOrCreate()


    val reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
    val measuresDS = reader.read(sql.Options(
      options.csvFilePath.get,
      Map(
        "inferSchema" -> "true",
        "delimiter" -> ",",
        "header" -> "true",
        "nameField" -> "metric_name",
        "timestampField" -> "timestamp",
        "timestampDateFormat" -> "s",
        "valueField" -> "value",
        "tagsFields" -> "metric_id,warn,crit"
      )))


    val chunkyfier = new Chunkyfier()
      .setGroupByCols(Array(  "name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .doDropLists(false)
      .setSaxAlphabetSize(options.saxAlphabetSize)
      .setSaxStringLength(options.saxStringLength)


    val chunksDS = chunkyfier.transform(measuresDS)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .repartition(1)


    val writer = WriterFactory.getChunksWriter(WriterType.SOLR)
    writer.write(sql.Options(options.collectionName, Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName,
      "tag_names" -> "metric_id,warn,crit"
    )), chunksDS)


    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.collectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString}")

    spark.close()
  }

}
