package com.hurence.historian.spark.sql

import java.io.File
import java.util

import com.hurence.historian.spark.ml.{Chunkyfier, UnChunkyfier}
import com.hurence.historian.spark.sql.reader.{ReaderFactory, ReaderType}
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.timeseries.model.{Chunk, Measure}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Encoders
import org.junit.Assert.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

@TestInstance(Lifecycle.PER_CLASS)
class ReaderWriterTests extends SparkSessionTestWrapper {



  private val logger = LoggerFactory.getLogger(classOf[ReaderWriterTests])




  val folder = FileUtils.getTempDirectory

  @BeforeAll
  def init(): Unit = {
    // to lazy load spark if needed
    spark.version
  }

  @Test
  def testReadWriteCsv() = {


    // 1. Read an existing csv file as a Dataset[Measure]
    val csvFilePath = new File(this.getClass.getClassLoader.getResource("it-data.csv").getPath)
    val initialDS = ReaderFactory.getMeasuresReader(ReaderType.CSV)
      .read(Options(
        csvFilePath.getAbsolutePath,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ",",
          "header" -> "true",
          "nameField" -> "name",
          "timestampField" -> "timestamp",
          "qualityField" -> "",
          "timestampDateFormat" -> "s",
          "valueField" -> "value",
          "tagsFields" -> "metric_id,warn,crit"
        )))
      .as[Measure]
    
    // 2. Write this DS to a temp csv file
    val createdFile = new File(folder.getAbsolutePath + "/out.csv")
    WriterFactory.getMeasuresWriter(WriterType.CSV)
      .write(Options(
        createdFile.getAbsolutePath,
        Map(
          "sep" -> ",",
          "quote" -> "\"",
          "header" -> "true",
          "tag_names" -> "metric_id,warn,crit"
        )), initialDS )


    // 3. Load this new file as a new Dataset[Measure]
    val newDS = ReaderFactory.getMeasuresReader(ReaderType.CSV)
      .read(Options(
        csvFilePath.getAbsolutePath,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ",",
          "header" -> "true",
          "nameField" -> "name",
          "timestampField" -> "timestamp",
          "timestampDateFormat" -> "s",
          "qualityField" -> "",
          "valueField" -> "value",
          "tagsFields" -> "metric_id,warn,crit"
        )))
      .as[Measure]

    // 4. Check if those datasets are equals
    assertEquals("The Datasets differs",
      initialDS.sort( "timestamp").collect().toList.asJava,
      newDS.sort("timestamp").collect().toList.asJava
    )

  }




  @Test
  def testLoadITDataMetricsParquet() = {

    implicit val measureEncoder = Encoders.bean(classOf[Measure])
    implicit val chunkEncoder = Encoders.bean(classOf[Chunk])

    val chunkyfier = new Chunkyfier()
      .setGroupByCols(Array("name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .doDropLists(false)
      .setSaxAlphabetSize(7)
      .setSaxStringLength(50)


    // load measures data with parquet
    val measuresDS = ReaderFactory.getMeasuresReader(ReaderType.PARQUET)
      .read(Options(this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath, Map()))
      .where("tags.metric_id LIKE '08%'")
      .as[Measure]

    // Chunkify measures
    val chunkifiedDS = chunkyfier.transform(measuresDS).as[Chunk]

    // test 1 : make sure we got back to original data
    val unchunkyfier = new UnChunkyfier()
    val rechunkifiedDS =  unchunkyfier.transform(chunkifiedDS)
      .as[Measure]

    if (logger.isDebugEnabled) {
      rechunkifiedDS.show()
    }

    assertEquals("The Datasets differs",
      measuresDS.sort( "timestamp").collect().toList.asJava,
      rechunkifiedDS.sort("timestamp").collect().toList.asJava
    )



  }





}
