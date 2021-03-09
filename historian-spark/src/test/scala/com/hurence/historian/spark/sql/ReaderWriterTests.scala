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
import org.junit.Assert.{assertEquals, assertTrue}
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



  @Test
  def testReadWriteCsvWithTags() = {


    // timestamp;tagname;value;quality;dataUPtag1;dataUPtag2;dataUPtag3;dataUPtag4
    // 1. Read an existing csv file as a Dataset[Measure]
    val csvFilePath = new File(this.getClass.getClassLoader.getResource("chemistry/echantillons_export.csv").getPath)
    val initialDS = ReaderFactory.getMeasuresReader(ReaderType.CSV)
      .read(Options(
        csvFilePath.getAbsolutePath,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ";",
          "header" -> "true",
          "nameField" -> "tagname",
          "timestampField" -> "timestamp",
          "qualityField" -> "quality",
          "timestampDateFormat" -> "dd/MM/yyyy HH:mm:ss",
          "valueField" -> "value",
          "tagsFields" -> "dataUPtag1,dataUPtag2,dataUPtag3,dataUPtag4"
        )))
      .as[Measure]


    val dataset = initialDS.sort( "timestamp").collect().toList


    // 4. Check if those datasets are equals
    assertEquals("The Dataset length is not correct",
      1295,
      dataset.length
    )

    // 04/03/2021 10:23:51;U248.TI121.F_CV;600.000000000;100.0;Temperature.Reacteur1_Coquille2.Interne_1.U248;Temperature.Reacteur1_Coquille2.Interne_1.U248-12;;
    dataset.foreach( measure => {
      if(measure.getName.equals("U248.TI121.F_CV")){
        assertEquals("bad tag", measure.getTag("dataUPtag1"),"Temperature.Reacteur1_Coquille2.Interne_1.U248" )
        assertEquals("bad tag", measure.getTag("dataUPtag2"),"Temperature.Reacteur1_Coquille2.Interne_1.U248-12" )
        assertEquals("bad value", measure.getValue,600.0, 0.001 )
        assertEquals("bad quality", measure.getQuality,100.0f, 0.001f )

      }
    })

  }




}
