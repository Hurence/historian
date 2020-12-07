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




  $SPARK_HOME/bin/spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:historian-spark/src/main/resources/log4j.properties" --class com.hurence.historian.spark.loader.FileLoader  historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar --config-file historian-spark/src/main/resources/fileloader-streaming-config.yml

    * @param args
    */
  def main(args: Array[String]): Unit = {

    // get arguments & setup spark session
   // val options = FileLoader.parseCommandLine(args)

    val options = if (args.size == 0)
      ConfigLoader.defaults()
    else
      ConfigLoader.loadFromFile(args(1))

    val appName = if (options.spark.streamingEnabled) "historian-loader-streaming" else "historian-loader-batch"
    val spark = SparkSession.builder
      .appName(appName)
      .master(options.spark.master)
      .config("spark.sql.shuffle.partitions",options.spark.sqlShufflePartitions)
      .getOrCreate()

    // run batch or stream
    if (options.spark.streamingEnabled) {
      runStreaming(options)
    }
    else {
      runBatch(options)
      spark.close()
    }

  }


  def runStreaming(options: FileLoaderConf): Unit = {

    val measuresDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_STREAM_CSV)
      .read(sql.Options(
        options.reader.csvFilePath,
        Map(
          "inferSchema" -> "true",
         // "schema" -> options.reader.schema,
          "header"-> "true",
          "maxFileAge" -> options.reader.maxFileAge,
          "maxFilesPerTrigger" -> s"${options.reader.maxFilesPerTrigger}",
          "delimiter" -> options.reader.columnDelimiter,
          "header" -> "true",
          "nameField" -> options.reader.nameField,
          "timestampField" -> options.reader.timestampField,
          "timestampDateFormat" -> options.reader.timestampFormat,
          "valueField" -> options.reader.valueField,
          "qualityField" -> options.reader.qualityField,
          "tagsFields" -> options.reader.tagNames,
          "mode" -> "DROPMALFORMED"
        )))

    val chunkyfier = new ChunkyfierStreaming()
      .setOrigin(options.chunkyfier.origin)
      .setGroupByCols(options.chunkyfier.groupByCols.split(","))
      .setDateBucketFormat(options.chunkyfier.dateBucketFormat)
      .setSaxAlphabetSize(options.chunkyfier.saxAlphabetSize)
      .setSaxStringLength(options.chunkyfier.saxStringLength)
      .setWindowDuration(options.chunkyfier.windowDuration)
      .setWatermarkDelayThreshold(options.chunkyfier.watermarkDelayThreshold)

    val writer = new SolrChunkForeachWriter(
      options.solr.zkHosts,
      options.solr.collectionName,
      options.solr.numConcurrentRequests,
      options.solr.batchSize,
      options.solr.flushInterval
    )

    val query = chunkyfier.transform(measuresDS)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .repartition(8)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", options.spark.checkpointDir)
      .foreach(writer)
      .start()

    query.awaitTermination()
  }

  def runBatch(options: FileLoaderConf): Unit = {
    logger.info(s"start batch loading files from : ${options.reader.csvFilePath}")

    // load CSV files as a DataSet
    val measuresDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
      .read(sql.Options(
        options.reader.csvFilePath,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> options.reader.columnDelimiter,
          "header" -> "true",
          "nameField" -> options.reader.nameField,
          "timestampField" -> options.reader.timestampField,
          "timestampDateFormat" -> options.reader.timestampFormat,
          "valueField" -> options.reader.valueField,
          "qualityField" -> options.reader.qualityField,
          "tagsFields" -> options.reader.tagNames
        )))

    // transform Measures into Chunks
    val chunksDS = new Chunkyfier()
      .setOrigin(options.chunkyfier.origin)
      .setGroupByCols(options.chunkyfier.groupByCols.split(","))
      .setDateBucketFormat(options.chunkyfier.dateBucketFormat)
      .setSaxAlphabetSize(options.chunkyfier.saxAlphabetSize)
      .setSaxStringLength(options.chunkyfier.saxStringLength)
      .transform(measuresDS)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .repartition(8)

    // write chunks to SolR
    WriterFactory.getChunksWriter(WriterType.SOLR)
      .write(sql.Options(options.solr.collectionName, Map(
        "zkhost" -> options.solr.zkHosts,
        "collection" -> options.solr.collectionName,
        "tag_names" -> options.reader.tagNames
      )), chunksDS)

    // explicit commit to make sure all docs are imediately visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.solr.zkHosts)
    val response = solrCloudClient.commit(options.solr.collectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString} to collection ${options.solr.collectionName}")

  }

}
