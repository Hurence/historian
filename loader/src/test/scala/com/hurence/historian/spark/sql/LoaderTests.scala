package com.hurence.historian.spark.sql

import com.hurence.historian.model.ChunkRecordV0
import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.historian.spark.sql.functions.{chunk, sax, guess}
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, MeasuresReaderType, ReaderFactory}
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LoaderTests extends SparkSessionTestWrapper {

  import spark.implicits._

  @Test
  def testMeasureV0() = {

    val sample = it4MetricsDS.groupBy($"name", $"tags.metric_id", $"day")
      .agg(
        collect_list($"value").as("values"),
        collect_list($"timestamp").as("timestamps"),
        count($"value").as("count"),
        avg($"value").as("avg"),
        min($"value").as("min"),
        max($"value").as("max"),
        first($"value").as("first"),
        last($"value").as("last"),
        stddev($"value").as("stddev"),
        first($"timestamp").as("start"),
        last($"timestamp").as("end"),
        first($"tags").as("tags"))
      .withColumn("value", chunk($"name", $"start", $"end", $"timestamps", $"values"))
      .withColumn("sax", sax(lit(7), lit(0.01), lit(50), $"values"))
      .select($"name", $"day", $"start", $"end", $"value", $"timestamps", $"values", $"count", $"avg", $"stddev", $"min", $"max", $"first", $"last", $"sax", $"tags")
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .as[ChunkRecordV0]
      .collect()



    println(sample(0))
    assertEquals(1574982082000L, sample(0).start)
    assertEquals(1575068166000L, sample(0).end)
    assertEquals("ack", sample(0).name)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", sample(0).tags("metric_id"))
    assertEquals("acbbbbacbcccbbaddeffgfffffgfgffeeeeeefeedebcbcbbcc", sample(0).sax)
    assertEquals("H4sIAAAAAAAAAFWWbUgUURSGdRVMSVKT8oeRQZCW0opCilZbWFA/+qAgoYI+NIwg+hEhBCVlpQa5pe3s7Mzk9gEJCUkUKWWYFhlUSBhUiJQsEWVUpBUk1c4sc569A8LZ43vf+77nnvuRllqwz/72aL609Lx317MKQnr0Kw740lJj4fugHT5/Fv2SnbDE+TZLOBiWsEO3wwT7GwX7g2x+QMgaQpLtCgj2F7ONaBLuckJH6ECbYP8FhKEBwAey15g4LyCAMQD+oIQ1zDaN9M8Buyy/O7JiIF/0P+kZdpVidmEqhf+nJrVbgLFlmoQaRZjQBVsOmRfdPlVAmSrgz3kB9gRZMbS8pC6LDQE0Y9uLrJuaZCMsf5/mCnBANzRXQKzYlOo4w8MYTMbKbRSu02XYVcIKlsAL9iJa5gXFzTgWvlHDV2hI0IWsi57KIOtDehtFuAq2iXUsJlyFyIVgc9GwHGX3kb4bQxYL3XlRDK1nWB7YKb8vzeMtdEEHGZpL3U6qS7Rdd5co1oNY9rMYEQQXAejX6eIOwc4AkI20ywg+TNn7QwK4o7uynKHhkCvLAdajJYPO/MKhsMOU7BpTBDQayn7INxTSOoYPmTBRqjam6gOQY7mkjvVzptLjiwH2MPyNJZoOIW+rxaFmKva7TUXpQoBhg56E6ZEpSzHHRD+ASYNjDDI/gGG1VNWWK8D5b/4lId1riauncUWh/kUImA0giO4qVr2cYQ+plhVXIhgihlL3Y6ZyuG1kTDsGJ8lGKNFPwrdxC2NIdvUlyZZanCIYWwp2jPAEDTUcEoYhZhuzhGFRNJvkDSfKpQVqnCmvYL+K2f9Qywnpm9iOUDs8G9LTVKIQ0k1kK+FvJes1uJMI54OdpSvHyRa1cUcpzouQhBtoxhrCTjbeElNpxsOqq0bsF8J/ISTD5wLIxqsXAVfotVMMq4BsbVyJAJzF9jYOpGHq8pUT0WPY63sr0Z0nE6OvkVdKGGH2tzTPBKJv0V11DBvFymOyfvZVOQwP4J1Gfzf6OxDZTgUKOdHLEDnuGOxMdH+3sKunQkpPPDGUnbqSy6QXS93cCk3IrEb83+iwFJvU25MYf+BugKMM+UGW6ih91cotNMKFnoKgZp5gKfjZSfZMO/uB220/vHHvgFp8NAGYyWyZQfu+rnQdDvD8+sSM93jGrGDGWtbkI++REvrvCC+PmrhnDNlOwjpqP91mS8pxCZM0++f26F9CdLGfyGlVisvv7XYftHjiN2x9ULk9uhwWj0t617G91P15gDI1YGVQt2m/y/7p1ZR3ynLdl5HwHzsmJESPDAAA", sample(0).value)

    assertEquals(288, sample(0).count)
    assertEquals(1651.561111111111, sample(0).avg)
    assertEquals(178.49958904923878, sample(0).stddev)
    assertEquals(68.8, sample(0).min)
    assertEquals(2145.6, sample(0).max)
    assertEquals(1496.6, sample(0).first)
    assertEquals(1615.4, sample(0).last)

  }

  @Test
  def testChunksV0() = {

    val sample = it4MetricsChunksDS
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      //.withColumn("sax2", sax(lit(5),lit( 0.05), lit(50), $"values"))
      .collect()

    println(sample(0))
    assertEquals(1574982082000L, sample(0).start)
    assertEquals(1575068166000L, sample(0).end)
    assertEquals("ack", sample(0).name)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", sample(0).tags("metric_id"))
    assertEquals("acbbbbacbcccbbaddeffgfffffgfgffeeeeeefeedebcbcbbcc", sample(0).sax)
    assertEquals("H4sIAAAAAAAAAFWWbUgUURSGdRVMSVKT8oeRQZCW0opCilZbWFA/+qAgoYI+NIwg+hEhBCVlpQa5pe3s7Mzk9gEJCUkUKWWYFhlUSBhUiJQsEWVUpBUk1c4sc569A8LZ43vf+77nnvuRllqwz/72aL609Lx317MKQnr0Kw740lJj4fugHT5/Fv2SnbDE+TZLOBiWsEO3wwT7GwX7g2x+QMgaQpLtCgj2F7ONaBLuckJH6ECbYP8FhKEBwAey15g4LyCAMQD+oIQ1zDaN9M8Buyy/O7JiIF/0P+kZdpVidmEqhf+nJrVbgLFlmoQaRZjQBVsOmRfdPlVAmSrgz3kB9gRZMbS8pC6LDQE0Y9uLrJuaZCMsf5/mCnBANzRXQKzYlOo4w8MYTMbKbRSu02XYVcIKlsAL9iJa5gXFzTgWvlHDV2hI0IWsi57KIOtDehtFuAq2iXUsJlyFyIVgc9GwHGX3kb4bQxYL3XlRDK1nWB7YKb8vzeMtdEEHGZpL3U6qS7Rdd5co1oNY9rMYEQQXAejX6eIOwc4AkI20ywg+TNn7QwK4o7uynKHhkCvLAdajJYPO/MKhsMOU7BpTBDQayn7INxTSOoYPmTBRqjam6gOQY7mkjvVzptLjiwH2MPyNJZoOIW+rxaFmKva7TUXpQoBhg56E6ZEpSzHHRD+ASYNjDDI/gGG1VNWWK8D5b/4lId1riauncUWh/kUImA0giO4qVr2cYQ+plhVXIhgihlL3Y6ZyuG1kTDsGJ8lGKNFPwrdxC2NIdvUlyZZanCIYWwp2jPAEDTUcEoYhZhuzhGFRNJvkDSfKpQVqnCmvYL+K2f9Qywnpm9iOUDs8G9LTVKIQ0k1kK+FvJes1uJMI54OdpSvHyRa1cUcpzouQhBtoxhrCTjbeElNpxsOqq0bsF8J/ISTD5wLIxqsXAVfotVMMq4BsbVyJAJzF9jYOpGHq8pUT0WPY63sr0Z0nE6OvkVdKGGH2tzTPBKJv0V11DBvFymOyfvZVOQwP4J1Gfzf6OxDZTgUKOdHLEDnuGOxMdH+3sKunQkpPPDGUnbqSy6QXS93cCk3IrEb83+iwFJvU25MYf+BugKMM+UGW6ih91cotNMKFnoKgZp5gKfjZSfZMO/uB220/vHHvgFp8NAGYyWyZQfu+rnQdDvD8+sSM93jGrGDGWtbkI++REvrvCC+PmrhnDNlOwjpqP91mS8pxCZM0++f26F9CdLGfyGlVisvv7XYftHjiN2x9ULk9uhwWj0t617G91P15gDI1YGVQt2m/y/7p1ZR3ynLdl5HwHzsmJESPDAAA", sample(0).value)

    assertEquals(288, sample(0).count)
    assertEquals(1651.561111111111, sample(0).avg)
    assertEquals(178.49958904923878, sample(0).stddev)
    assertEquals(68.8, sample(0).min)
    assertEquals(2145.6, sample(0).max)
    assertEquals(1496.6, sample(0).first)
    assertEquals(1615.4, sample(0).last)


  }


  @Test
  def testChunksV0Sax() = {


    // simply recompute sax string ith new parameters
    val sample = it4MetricsChunksDS
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .withColumn("sax", sax(lit(5), lit(0.05), lit(50), $"values"))
      .as[ChunkRecordV0]
      .collect()

    assertEquals("acabaaabbbbbbaaccdddeeeedeedeeddddddddddcdbbbbabbb", sample(0).sax)


  }


  @Test
  def testChunksV0Guess() = {


    // simply recompute sax string ith new parameters
    val sample = it4MetricsChunksDS
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .withColumn("guess", guess(lit(5), $"values"))
      .as[ChunkRecordV0]
      .collect()

    assertEquals("acabaaabbbbbbaaccdddeeeedeedeeddddddddddcdbbbbabbb", sample(0).sax)


  }



  @Test
  def testLoaderCSV() ={

    def config(): Map[String, String] = Map(
      "inferSchema" -> "true",
      "delimiter" -> ",",
      "header" -> "true",
      "dateFormat" -> ""
    )

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.csv.gz").getPath

    spark.read
      .options(config)
      .csv(filePath)
      .show()
  }


  @Test
  def testLoadITDataCSVV0() ={

    spark.version

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
  }

  @Test
  def testLoadITDataParquetV0() ={

    spark.version

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath
    val options = Options( filePath, Map())
    val itDataV0Reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.PARQUET)

    val ds = itDataV0Reader.read(options)


    ds.printSchema()
    ds.show()

  }


  @Test
  def testLoadITDataChunksParquetV0() ={

    spark.version

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath
    val options = Options( filePath, Map())
    val reader = ReaderFactory.getChunksReader(ChunksReaderType.PARQUET)
    val ds = reader.read(options)

    ds.printSchema()
    ds.show()

  }
}
