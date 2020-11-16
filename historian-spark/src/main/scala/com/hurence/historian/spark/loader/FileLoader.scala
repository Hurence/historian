package com.hurence.historian.spark.loader

import com.hurence.historian.service.SolrChunkService
import com.hurence.historian.spark.ml.{Chunkyfier, ChunkyfierStreaming}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader.{MeasuresReaderType, ReaderFactory}
import com.hurence.historian.spark.sql.writer.solr.SolrChunkForeachWriter
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{GnuParser, Option, Options}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.OutputMode
import org.slf4j.LoggerFactory


class FileLoader extends Serializable {
}

object FileLoader {

  val DEFAULT_CHUNK_SIZE = 1440
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 24
  val DEFAULT_GROUP_BY_COLS = "name"
  val DEFAULT_TIMESTAMP_FIELD = "timestamp"
  val DEFAULT_NAME_FIELD = "name"
  val DEFAULT_VALUE_FIELD = "value"
  val DEFAULT_QUALITY_FIELD = ""
  val DEFAULT_TIMESTAMP_FORMAT = "s"
  val DEFAULT_CSV_COLUMN_DELIMITER = ","
  val DEFAULT_ORIGIN = "file_loader"
  val DEFAULT_DATE_BUCKET_FORMAT = "yyyy-MM-dd"
  val DEFAULT_STREAMING_ENABLED = false
  val DEFAULT_CHECKOINT_DIR = "checkpoint/file-loader"


  def buildOption(opt: String, longOpt: String, hasArg: Boolean, optionalArg: Boolean, description: String) = {
    val option = new Option(opt, longOpt, hasArg, description)
    option.setOptionalArg(optionalArg)
    option
  }

  case class FileLoaderOptions(master: String,
                               zkHosts: String,
                               collectionName: String,
                               csvFilePath: scala.Option[String],
                               parquetFilePath: scala.Option[String],
                               chunkSize: Int,
                               saxAlphabetSize: Int,
                               saxStringLength: Int,
                               useKerberos: Boolean,
                               tagNames: String,
                               groupByCols: String,
                               timestampField: String,
                               nameField: String,
                               valueField: String,
                               qualityField: String,
                               timestampFormat: String,
                               columnDelimiter: String,
                               origin: String,
                               dateBucketFormat: String,
                               streamingEnabled: Boolean,
                               checkpointLocation: String)


  /**
    * transform commands line args into FileLoaderOptions
    *
    * @param args
    * @return
    */
  def parseCommandLine(args: Array[String]): FileLoaderOptions = {
    val parser = new GnuParser()
    val options = new Options


    val helpMsg = "Print this message."
    val help = new Option("help", helpMsg)
    options.addOption(help)

    options.addOption(
      buildOption("ms", "spark-master", true, false, "spark master")
    )
    options.addOption(
      buildOption("zk", "zookeeper-quorum", true, false, s"the zookeeper quorum for solr collection")
    )
    options.addOption(
      buildOption("col", "collection-name", true, false, s"Solr collection name, default historian")
    )
    options.addOption(
      buildOption("csv", "csv-file-path", true, true, s"File path mask, can be anything like /a/B/c/*/*pr*/*.csv")
    )
    options.addOption(
      buildOption("pq", "parquet-file-path", true, true, s"File path mask, can be anything like /a/B/c/*/*pr*/*.parquet")
    )
    options.addOption(
      buildOption("cs", "chunks-size", true, true, s"num measures in a chunk, default $DEFAULT_CHUNK_SIZE")
    )
    options.addOption(
      buildOption("sas", "sax-alphabet-size", true, true, s"size of alphabet, default $DEFAULT_SAX_ALPHABET_SIZE")
    )
    options.addOption(
      buildOption("ssl", "sax-string-length", true, true, s"num measures in a chunk, default $DEFAULT_SAX_STRING_LENGTH")
    )
    options.addOption(
      buildOption("kb", "kerberos", true, true, "do we use kerberos ?, default false")
    )
    options.addOption(
      buildOption("date", "recompaction-date", true, true, "the day date to recompact in the form of yyyy-MM-dd")
    )
    options.addOption(
      buildOption("tags", "tags-names", true, false, "the columns to read as tags as a csv string")
    )
    options.addOption(
      buildOption("groupBy", "groupby-cols", true, true, s"the column names that form the group by key as a csv string, default to $DEFAULT_GROUP_BY_COLS")
    )
    options.addOption(
      buildOption("ts", "timestamp-field", true, true, s"the column name that handles the timestamp, default to $DEFAULT_TIMESTAMP_FIELD")
    )
    options.addOption(
      buildOption("name", "name-field", true, true, s"the column name that handles the metric name, default to $DEFAULT_NAME_FIELD")
    )
    options.addOption(
      buildOption("value", "value-field", true, true, s"the column name that handles the metric value, default to $DEFAULT_VALUE_FIELD")
    )
    options.addOption(
      buildOption("tf", "timestamp-format", true, true, s"the format of timestamp conversion, can be java date pattern or s or ms, default to $DEFAULT_TIMESTAMP_FORMAT")
    )
    options.addOption(
      buildOption("cd", "column-delimiter", true, true, s"the char delimiter for a column, default to $DEFAULT_CSV_COLUMN_DELIMITER")
    )
    options.addOption(
      buildOption("quality", "quality-field", true, true, s"the column name that handles the metric quality, default to $DEFAULT_QUALITY_FIELD")
    )
    options.addOption(
      buildOption("origin", "origin", true, true, s"the origin name, default to $DEFAULT_ORIGIN")
    )
    options.addOption(
      buildOption("dbf", "date-format-bucket", true, true, s"the date pattern to broup by measures like by day, hour, ... $DEFAULT_DATE_BUCKET_FORMAT")
    )
    options.addOption(
      buildOption("stream", "streaming-enabled", false, true, s"do we stream files continously from folder $DEFAULT_STREAMING_ENABLED")
    )
    options.addOption(
      buildOption("checkpoint", "checkpoint-location", true, true, s"spark checkpointing folder $DEFAULT_CHECKOINT_DIR")
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
    val tagNames = if (line.hasOption("tags")) line.getOptionValue("tags") else ""
    val groupByCols = if (line.hasOption("groupBy")) line.getOptionValue("groupBy") else DEFAULT_GROUP_BY_COLS
    val timestampField = if (line.hasOption("ts")) line.getOptionValue("ts") else DEFAULT_TIMESTAMP_FIELD
    val nameField = if (line.hasOption("name")) line.getOptionValue("name") else DEFAULT_NAME_FIELD
    val valueField = if (line.hasOption("value")) line.getOptionValue("value") else DEFAULT_VALUE_FIELD
    val qualityField = if (line.hasOption("quality")) line.getOptionValue("quality") else DEFAULT_QUALITY_FIELD
    val timestampFormat = if (line.hasOption("tf")) line.getOptionValue("tf") else DEFAULT_TIMESTAMP_FORMAT
    val columnDelimiter = if (line.hasOption("cd")) line.getOptionValue("cd") else DEFAULT_CSV_COLUMN_DELIMITER
    val origin = if (line.hasOption("origin")) line.getOptionValue("origin") else DEFAULT_ORIGIN
    val dateBucketFormat = if (line.hasOption("dbf")) line.getOptionValue("dbf") else DEFAULT_DATE_BUCKET_FORMAT
    val streamingEnabled = if (line.hasOption("stream")) true else DEFAULT_STREAMING_ENABLED
    val checkpointLocation = if (line.hasOption("checkpoint")) line.getOptionValue("checkpoint") else DEFAULT_CHECKOINT_DIR

    // build the option handler
    val opts = FileLoaderOptions(sparkMaster,
      zkHosts,
      collectionName,
      csvFilePath,
      parquetFilePath,
      chunksSize,
      alphabetSize,
      saxStringLength,
      useKerberos,
      tagNames,
      groupByCols,
      timestampField,
      nameField,
      valueField,
      qualityField,
      timestampFormat,
      columnDelimiter,
      origin,
      dateBucketFormat,
      streamingEnabled,
      checkpointLocation
    )

    logger.info(s"Command line options : $opts")
    opts
  }

  private val logger = LogManager.getLogger(classOf[SolrChunkService])


  /**
    * 4' by day for S35 : 35M raw zip / 250M raw / 50M index
    *
    *
    * $SPARK_HOME/bin/spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:historian-spark/src/main/resources/log4j.properties" --class com.hurence.historian.spark.loader.FileLoader  --jars  historian-resources/jars/spark-solr-3.6.6-shaded.jar,historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar   historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar  -csv "data/chemistry/dataHistorian-ISNTS35-N-20200301*.csv"  -groupBy name -zk localhost:9983 -col historian -name tagname -cd ";"  -tags tagname -quality quality -tf "dd/MM/yyyy HH:mm:ss" -origin chemistry -dbf "yyyy-MM-dd.HH"
    *
    *
    * $SPARK_HOME/bin/spark-submit --driver-java-options '-Dlog4j.configuration=file:historian-spark/src/main/resources/log4j.properties' --class com.hurence.historian.spark.loader.FileLoader --jars  historian-resources/jars/spark-solr-3.6.6-shaded.jar,historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar  historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar -csv historian-spark/src/test/resources/it-data-4metrics.csv.gz -tags metric_id -groupBy name,tags.metric_id -zk localhost:9983 -name metric_name -origin it-data
    *
    *
  $SPARK_HOME/bin/spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:historian-spark/src/main/resources/log4j.properties" --class com.hurence.historian.spark.loader.FileLoader  --jars  historian-resources/jars/spark-solr-3.6.6-shaded.jar,historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar   historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar  -csv "data/chemistry/_in"  -groupBy name -zk localhost:9983 -col historian -name tagname -cd ";"  -tags tagname -quality quality -tf "dd/MM/yyyy HH:mm:ss" -origin chemistry -dbf "yyyy-MM-dd.HH" -stream

    * @param args
    */
  def main(args: Array[String]): Unit = {

    // get arguments & setup spark session
    val options = FileLoader.parseCommandLine(args)
    val appName = if (options.streamingEnabled) "historian-loader-streaming" else "historian-loader-batch"
    val spark = SparkSession.builder
      .appName(appName)
      .master(options.master)
      .getOrCreate()

    // run batch or stream
    if (options.streamingEnabled) {
      runStreaming(options)
    }
    else {
      runBatch(options)
      spark.close()
    }

  }


  def runStreaming(options: FileLoaderOptions): Unit = {

    val measuresDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_STREAM_CSV)
      .read(sql.Options(
        options.csvFilePath.get,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> options.columnDelimiter,
          "header" -> "true",
          "nameField" -> options.nameField,
          "timestampField" -> options.timestampField,
          "timestampDateFormat" -> options.timestampFormat,
          "valueField" -> options.valueField,
          "qualityField" -> options.qualityField,
          "tagsFields" -> options.tagNames
        )))

    val chunkyfier = new ChunkyfierStreaming()
      .setOrigin(options.origin)
      .setGroupByCols(options.groupByCols.split(","))
      .setDateBucketFormat(options.dateBucketFormat)
      .setSaxAlphabetSize(options.saxAlphabetSize)
      .setSaxStringLength(options.saxStringLength)

    val writer = new SolrChunkForeachWriter(options.zkHosts, options.collectionName)

    val query = chunkyfier.transform(measuresDS)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .repartition(8)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", options.checkpointLocation)
      .foreach(writer)
      .start()

    query.awaitTermination()
  }

  def runBatch(options: FileLoaderOptions): Unit = {
    logger.info(s"start batch loading files from : ${options.csvFilePath.get}")

    // load CSV files as a DataSet
    val measuresDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
      .read(sql.Options(
        options.csvFilePath.get,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> options.columnDelimiter,
          "header" -> "true",
          "nameField" -> options.nameField,
          "timestampField" -> options.timestampField,
          "timestampDateFormat" -> options.timestampFormat,
          "valueField" -> options.valueField,
          "qualityField" -> options.qualityField,
          "tagsFields" -> options.tagNames
        )))

    // transform Measures into Chunks
    val chunksDS = new Chunkyfier()
      .setOrigin(options.origin)
      .setGroupByCols(options.groupByCols.split(","))
      .setDateBucketFormat(options.dateBucketFormat)
      .setSaxAlphabetSize(options.saxAlphabetSize)
      .setSaxStringLength(options.saxStringLength)
      .transform(measuresDS)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .repartition(8)

    // write chunks to SolR
    WriterFactory.getChunksWriter(WriterType.SOLR)
      .write(sql.Options(options.collectionName, Map(
        "zkhost" -> options.zkHosts,
        "collection" -> options.collectionName,
        "tag_names" -> options.tagNames
      )), chunksDS)

    // explicit commit to make sure all docs are imediately visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.collectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString} to collection ${options.collectionName}")

  }

}
