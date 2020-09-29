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
    assertEquals("H4sIAAAAAAAAAGWWe0yNcRjHK1GdFWlsRbPmMtpcSnIZy8lGG5vhD0LDUGzmMncrl2SMv1p1znkvJ2KmDLWpzNjEhqKSyz+kTTZbNresRrSZ933zPp8fzl/PeX7f5/t9Lr/L64lKMHTrdzjg9cQkhNi/zbo3pbMyzvNnpdDvtcw0+/das0GOmRXwpvSdi/PExFrQhNYW61emed2Ye0pMmW3m2b8wB+BozNYE0BgQb4dfsBlgeyGrDAjgiV/UMhEeHBDsdxgSHa+T5GQYSjQxVyK8PyBk9eUSdldRUyTwJiMRjnC25Q1LinK5N5JoOlXXQdgAYB9mDM3apIs5jPSH6GL+hPeYLmY3EmudGTq8p/+aoRN+BNKZARn2BwvYVPMXcAzAO7T0F6UX0MfBYNdR1Wpy2kKB+ZANBdBPo9NhqAdQBMMVJhFNt/rxaiTpdeYzwiX8xMo8zV6Z5K4k+CStHbow7aVF4/T/zkOIwVyZShOJBEl6LqRNuhR4nmY8p5RSZ4IONtZUVZ3V9fZqrDWtAYofhlddvY1OLzllI3nZzz5GcglhBYaYLYaE5WIOB9DLrFoh2+IT4SoAr0wxn5mCTYM3KsgIDHs461yWYkMIkwC1G2LuVqpmKM2YjTTgEd5Scm6A7CRk75llB4mm0oD77PwVMDxXhDFTK8TsAnuG2jqDIjEH7wyaVYO3wBSGRUEBzCd1zbRbuMr9G49iO1W3wV3KDXkd7hsM7S130C2GUA3vT10unnJD3bYD9ycxl/yyvw+a/52qE+SURUN8eKcF7cryXbbHDCadZEpIfAKhS4LSn6sM8WKQwwvZPQA5pgAq8B5jtF305jPdrbUAg5KOh7qxQWK9ZBpGt6fqdkBFqEtgsmu3k0IZYkuVW8XPGSGxA5hfFDLneG118RuU3QjoMIW8wTuFS6UX8REAlkO2GO8DsN2k/1DZriRay7N2UzkGup3zIamcXr4k0R7wH5Fpp3fjkdnDqD/BcIr01xB2n5Ti2U35eHchfJGqC1GbZUq+dcr7gkS3Jt5dAN7BG45ZB+9RqsgE0ERBRXgTUfuoFK9cKhR/FjPDMiOSqkL/fYhGk/FTHroarswW3oFIAMPIbSfeibzMy2hrDp0oZp5jAfTxgE6n7wvh3eZsm0TrWH2VY9WsBPkE+QJvtU+orvGApSIQBdajTJ7PlRf0phPsWOa2Hu8Csm3jMyiXNqWRQyHvzQUkUmheh/IpidmDWp5m3zHfQ62ujHR93zjUxWglO19I0aMiIkNGWh+bDb8B4n7pV0oMAAA=", chunktoCheck.getValueAsString)

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
