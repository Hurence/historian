package com.hurence.historian.spark.sql

import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql.functions.{anomalie_test, chunk, guess, sax, sax_best_guess, sax_best_guess_paa_fixed}
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, MeasuresReaderType, ReaderFactory}
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.model.Chunk
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

@TestInstance(Lifecycle.PER_CLASS)
class LoaderTests extends SparkSessionTestWrapper {

  import spark.implicits._

  private val logger = LoggerFactory.getLogger(classOf[LoaderTests])


  @BeforeAll
  def init(): Unit = {
    // to lazy load spark if needed
    spark.version
  }



  @Test
  def testChunkyfier() = {

    val chunkyfier = new Chunkyfier()
      .setGroupByCols(Array("name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .doDropLists(false)
      .setSaxAlphabetSize(7)
      .setSaxStringLength(50)

    it4MetricsDS.show()

    // Transform original data into its bucket index.
    val sample = chunkyfier.transform(it4MetricsDS)
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .collect()


    checkChunk(sample(0))
  }

  def checkChunk(chunktoCheck: Chunk) = {
    assertEquals(1575068466000L, chunktoCheck.getStart)
    assertEquals(1575154561000L, chunktoCheck.getEnd)
    assertEquals("ack", chunktoCheck.getName)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", chunktoCheck.getTag("metric_id"))
    assertEquals("baaabbcbbbbbbcdcedffefgfffffgffdedeefffefebdbacbbb", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)

    assertEquals(288, chunktoCheck.getCount)
    assertEquals(1620.0979166666662, chunktoCheck.getAvg)
    assertEquals(104.87785626299073, chunktoCheck.getStdDev)
    assertEquals(1334.8, chunktoCheck.getMin)
    assertEquals(2072.6, chunktoCheck.getMax)
    assertEquals(1503.4, chunktoCheck.getFirst)
    assertEquals(1551.6, chunktoCheck.getLast)
  }



  @Test
  def testLoaderCSV() = {

    val reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.csv.gz").getPath
    val options = Options(
      filePath,
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
      ))

    val ds = reader.read(options)

    if (logger.isDebugEnabled) {
      ds.show()
    }

  }


  @Test
  def testLoadITDataCSVV0() = {

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.csv.gz").getPath
    val options = Options(
      filePath,
      Map(
        "inferSchema" -> "true",
        "delimiter" -> ",",
        "header" -> "true",
        "dateFormat" -> ""
      ))
    val itDataV0Reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.ITDATA_CSV)

    val ds = itDataV0Reader.read(options)

    ds.show()
    if (logger.isDebugEnabled) {
      ds.show()
    }
  }

  @Test
  def testLoadITDataParquetV0() = {

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath
    val options = Options(filePath, Map())
    val itDataV0Reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.PARQUET)

    val ds = itDataV0Reader.read(options)

    if (logger.isDebugEnabled) {
      ds.printSchema()
      ds.show()
    }

  }


  @Test
  def testLoadITDataChunksParquetV0() = {

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath
    val options = Options(filePath, Map())
    val reader = ReaderFactory.getChunksReader(ChunksReaderType.PARQUET)
    val ds = reader.read(options)

    if (logger.isDebugEnabled) {
      ds.printSchema()
      ds.show()
    }

  }


}
