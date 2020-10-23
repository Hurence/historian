package com.hurence.historian.spark.sql

import java.util

import com.hurence.historian.spark.{DataFrameComparer, DatasetContentMismatch, SparkSessionTestWrapper}
import com.hurence.historian.spark.ml.{Chunkyfier, UnChunkyfier}
import com.hurence.historian.spark.sql.functions._
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, MeasuresReaderType, ReaderFactory}
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.converter.{MeasuresToChunk, MeasuresToChunkVersionCurrent}
import com.hurence.timeseries.model.{Chunk, Measure}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.scalatest.Matchers.intercept
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

@TestInstance(Lifecycle.PER_CLASS)
class ReaderWriterTests extends SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  private val logger = LoggerFactory.getLogger(classOf[ReaderWriterTests])


  @BeforeAll
  def init(): Unit = {
    // to lazy load spark if needed
    spark.version
  }


  @Test
  def testMultipleCSVReaderForITData() = {
    val csvFilePath = this.getClass.getClassLoader.getResource("it-data-4metrics.csv.gz").getPath

    // load IT data with generic CSV reader
    val genericCSVReaderDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
      .read(Options(
        csvFilePath,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ",",
          "header" -> "true",
          "nameField" -> "metric_name",
          "timestampField" -> "timestamp",
          "qualityField" -> "",
          "timestampDateFormat" -> "s",
          "valueField" -> "value",
          "tagsFields" -> "metric_id,warn,crit"
        )))

    // load IT data with specific CSV reader
    val itDataCSVReaderDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.ITDATA_CSV)
      .read(Options(
        csvFilePath,
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ",",
          "header" -> "true",
          "dateFormat" -> ""
        )))

    // compare those 2 datasets
    val e1 = intercept[DatasetContentMismatch] {
      assertSmallDatasetEquality(
        genericCSVReaderDS,
        itDataCSVReaderDS
      )
    }

    // load same data with parquet
    val parquetFilePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath
    val itDataParquetReaderDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.PARQUET)
      .read(Options(parquetFilePath, Map()))

    // compare those 2 datasets
    val e2 = intercept[DatasetContentMismatch] {
      assertSmallDatasetEquality(
        genericCSVReaderDS,
        itDataParquetReaderDS
      )
    }

  }


  @Test
  def testLoadITDataChunksParquetV0() = {

    implicit val measureEncoder = Encoders.bean(classOf[Measure])
    implicit val chunkEncoder = Encoders.bean(classOf[Chunk])

    val chunkyfier = new Chunkyfier()
      .setGroupByCols(Array("name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .doDropLists(false)
      .setSaxAlphabetSize(7)
      .setSaxStringLength(50)

    val unchunkyfier = new UnChunkyfier()

    // load measures data with parquet
    val measuresDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.PARQUET)
      .read(Options(this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath, Map()))
      .where("tags.metric_id LIKE '08%'")
      .as[Measure]

    // Chunkify measures
    val chunkifiedDS = chunkyfier.transform(measuresDS)
      .as[Chunk]

    // test 1 : make sure we got back to original data
    val rechunkifiedDS =  unchunkyfier.transform(chunkifiedDS)
      .as[Measure]

    rechunkifiedDS.show()

    assertSmallDatasetEquality(
      measuresDS.sort("timestamp"),
      rechunkifiedDS.sort("timestamp")
    )

/*
    val chunksDS = ReaderFactory.getChunksReader(ChunksReaderType.PARQUET)
      .read(Options(this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath, Map()))
      .as[Chunk](Encoders.bean(classOf[Chunk]))

    chunksDS.show()

    // compare those 2 datasets

      assertSmallDatasetEquality(
        chunkifiedDS,
        chunksDS,
        false,
        false,
        true
      )*/

  }

  @Test
  def chunkyfierTest()= {


    // build a bunch of random measures
    val name = "metric"
    val tags = new util.HashMap[String, String]() {}
    val inputMeasures = ListBuffer[Measure]()
    for (i <- 0 until 10000) {
      inputMeasures += (randomMeasure(name, tags, 0, 100, "1977-03-02"))
    }

    val ds = spark.sparkContext.parallelize( inputMeasures).toDS()
    // convert them as a Chunk



    val chunkyfier = new Chunkyfier()
      .setOrigin("chunkyfierTest")
      .setGroupByCols("name".split(","))
      .setDateBucketFormat("yyyy-MM-dd.HH")
      .setSaxAlphabetSize(7)
      .setSaxStringLength(24)


    val chunksDS = chunkyfier.transform(ds)


    chunksDS.show(30)

    /*val converter = new MeasuresToChunkVersionCurrent("chunkyfierTest")
    val chunk = converter.buildChunk(name, inputMeasures, tags)*/
  }

}
