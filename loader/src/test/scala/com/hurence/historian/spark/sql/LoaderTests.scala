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
  def init() : Unit= {
    // to lazy load spark if needed
    spark.version
  }


  @Test
  def testMeasureV0() = {
    implicit val chunkEncoder = Encoders.bean(classOf[Chunk])

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
        stddev($"value").as("std_dev"),
        first($"timestamp").as("start"),
        last($"timestamp").as("end"),
        first($"tags").as("tags"))
      .withColumn("chunk", chunk($"name", $"start", $"end", $"timestamps", $"values"))
      .withColumn("sax", sax(lit(7), lit(0.01), lit(50), $"values"))
      .select($"name", $"day", $"start", $"end", $"chunk", $"timestamps", $"values", $"count", $"avg", $"std_dev", $"min", $"max", $"first", $"last", $"sax", $"tags")
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      //.as[Chunk]
      .collect()


    logger.debug(sample(0).toString);
  /*  assertEquals(1574982082000L, sample(0).getStart)
    assertEquals(1575068166000L, sample(0).getEnd)
    assertEquals("ack", sample(0).getName)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", sample(0).getTag("metric_id"))
    assertEquals("acbbbbacbcccbbaddeffgfffffgfgffeeeeeefeedebcbcbbcc", sample(0).getSax)
    assertEquals("H4sIAAAAAAAAAFWWbUgUURSGdRVMSVKT8oeRQZCW0opCilZbWFA/+qAgoYI+NIwg+hEhBCVlpQa5pe3s7Mzk9gEJCUkUKWWYFhlUSBhUiJQsEWVUpBUk1c4sc569A8LZ43vf+77nnvuRllqwz/72aL609Lx317MKQnr0Kw740lJj4fugHT5/Fv2SnbDE+TZLOBiWsEO3wwT7GwX7g2x+QMgaQpLtCgj2F7ONaBLuckJH6ECbYP8FhKEBwAey15g4LyCAMQD+oIQ1zDaN9M8Buyy/O7JiIF/0P+kZdpVidmEqhf+nJrVbgLFlmoQaRZjQBVsOmRfdPlVAmSrgz3kB9gRZMbS8pC6LDQE0Y9uLrJuaZCMsf5/mCnBANzRXQKzYlOo4w8MYTMbKbRSu02XYVcIKlsAL9iJa5gXFzTgWvlHDV2hI0IWsi57KIOtDehtFuAq2iXUsJlyFyIVgc9GwHGX3kb4bQxYL3XlRDK1nWB7YKb8vzeMtdEEHGZpL3U6qS7Rdd5co1oNY9rMYEQQXAejX6eIOwc4AkI20ywg+TNn7QwK4o7uynKHhkCvLAdajJYPO/MKhsMOU7BpTBDQayn7INxTSOoYPmTBRqjam6gOQY7mkjvVzptLjiwH2MPyNJZoOIW+rxaFmKva7TUXpQoBhg56E6ZEpSzHHRD+ASYNjDDI/gGG1VNWWK8D5b/4lId1riauncUWh/kUImA0giO4qVr2cYQ+plhVXIhgihlL3Y6ZyuG1kTDsGJ8lGKNFPwrdxC2NIdvUlyZZanCIYWwp2jPAEDTUcEoYhZhuzhGFRNJvkDSfKpQVqnCmvYL+K2f9Qywnpm9iOUDs8G9LTVKIQ0k1kK+FvJes1uJMI54OdpSvHyRa1cUcpzouQhBtoxhrCTjbeElNpxsOqq0bsF8J/ISTD5wLIxqsXAVfotVMMq4BsbVyJAJzF9jYOpGHq8pUT0WPY63sr0Z0nE6OvkVdKGGH2tzTPBKJv0V11DBvFymOyfvZVOQwP4J1Gfzf6OxDZTgUKOdHLEDnuGOxMdH+3sKunQkpPPDGUnbqSy6QXS93cCk3IrEb83+iwFJvU25MYf+BugKMM+UGW6ih91cotNMKFnoKgZp5gKfjZSfZMO/uB220/vHHvgFp8NAGYyWyZQfu+rnQdDvD8+sSM93jGrGDGWtbkI++REvrvCC+PmrhnDNlOwjpqP91mS8pxCZM0++f26F9CdLGfyGlVisvv7XYftHjiN2x9ULk9uhwWj0t617G91P15gDI1YGVQt2m/y/7p1ZR3ynLdl5HwHzsmJESPDAAA", sample(0).getValueAsString)

    assertEquals(288, sample(0).getCount)
    assertEquals(1651.561111111111, sample(0).getAvg)
    assertEquals(178.49958904923878, sample(0).getStd)
    assertEquals(68.8, sample(0).getMin)
    assertEquals(2145.6, sample(0).getMax)
    assertEquals(1496.6, sample(0).getFirst)
    assertEquals(1615.4, sample(0).getLast)*/
  }

  @Test
  def testChunkyfier() = {


    val chunkyfier = new Chunkyfier()
      .setValueCol("value")
      .setTimestampCol("timestamp")
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


    logger.debug(sample(0).toString)
    assertEquals(1574982082000L, sample(0).getStart)
    assertEquals(1575068166000L, sample(0).getEnd)
    assertEquals("ack", sample(0).getName)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", sample(0).getTag("metric_id"))
    assertEquals("acbbbbacbcccbbaddeffgfffffgfgffeeeeeefeedebcbcbbcc", sample(0).getSax)
    assertEquals("H4sIAAAAAAAAAGWWe0gUURTGdzfBWpLWkkow2iBIbMONFlJ6TWFB/dGDgoIKemgYQfRHRBCU9DSD2lJ3Z3Zm2i2DFhISKVJ60BMNKiQKKkRKlogyKnqCUu2M7fkNzYBw9vjd737fuec+/KOKt1nflrjiLyhOaNlvZkwJv7441j9q+OcbVcmGjx9lvzw7nGV/qyS8l5IwqVmhx/p6wX4lWxoTsrqEZFtjgv3JbM/iEm6yQ1vo3UbB/okJQx2At2QvMHEwJoA+AFFVwmpmG0L6h5hVFhugqEr4V3KsvyCQLc4/uzBF4P9hM9m1m4Kx2XEJ4xRhQBNsJWRhdCsIqHALGDwtwA5VmN6g5Sl1KdMFcBzbYWRdjks2ExPsTbsxbMCluFPAcLEp1QGGpzCYh5UrKFyqybAWwjksQRhsM1omqeKmHwufqeFzNHg0IWulpwJkFaQ3UoQWsPWs40zChYicCrYEDfNQdgPpmzFkstDpZjG0jGFBsN+jit8XDOVAOxlaQt0OsUTrNecSDfcglqMsRgbBMwDc1ujipGBHAihC2jkE76bstxMCuKpZsuxhqYRTlg3ci5YAnfmRQ2GDIdnFhgg4rMt+KNVdpLUM7zZgolSNTHUTwETTIrVtnzRcPV4GsIPhL03RtAt5a0wONUPstxkupVMBpnR6Eqb7hizFeAP9AL7pHGOQRQH0UKp1plOA/d/Ss0K61RRXDx1Fof4zEDAOgIruKla9kmF3qJbpKBEMGV3qvt9wHW4rGNOEwW9kM5ToB+Erx8Lokl10VrIRk1MEY+Vg+wgP0lA9CWHoZrY+UximZbMjgimvXFqg+pnyPParmH2QWg4YchN/dHd4EaRHqUQI0pVk58J/imxY504inAx2jCbHyWp34/ZSnCcJCZfTjNWEaTbedEOacbfb1WHsh+A/k5DhEwAU4TWMgPP02hGGzYFsiaNEAE5gey0HUg91+cSJ6NOt9W335uYpxOgL5EUIM8z+iuYZQHQ73VXLsF6sPCAbZV9VwnAL3iH0t6E/icgmKhDiRK9AZL9tMO3N/W5gV39PSE906a6duoDLpBNLbdwK9chch/jf2WH5wQ7v/wfucjgqkK+yVPvoq1PcQs+40PMRdJwnWD5+NpI91sR+4HbbDq/jHVCDj3oAo5mtULXu67k5h3d5fr1nxus8Y+YzYw1r8o73yCz6bw8vj2rHM4ZsmrCW2g81WpIm5ghHxK2f67N/nuxid8lpFcHllyarDxp8uQ27V3XdHq02iy9Hes22XZ77uYMy1WHlnmbRfpH902k9W3inzNOUgOcvC7L81I8MAAA=", sample(0).getValueAsString)
    assertEquals(288, sample(0).getCount)
    assertEquals(1651.561111111111, sample(0).getAvg)
    assertEquals(178.49958904923878, sample(0).getStd_dev)
    assertEquals(68.8, sample(0).getMin)
    assertEquals(2145.6, sample(0).getMax)
    assertEquals(1496.6, sample(0).getFirst)
    assertEquals(1615.4, sample(0).getLast)
  }

  @Test
  def testChunksV0() = {

    if (logger.isDebugEnabled) {
      it4MetricsChunksDS.show()
    }


    val sample = it4MetricsChunksDS
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      //.withColumn("sax2", sax(lit(5),lit( 0.05), lit(50), $"values"))
      .collect()

    logger.debug(sample(0).toString)
    assertEquals(1574982082000L, sample(0).getStart)
    assertEquals(1575068166000L, sample(0).getEnd)
    assertEquals("ack", sample(0).getName)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", sample(0).getTag("metric_id"))
    assertEquals("acbbbbacbcccbbaddeffgfffffgfgffeeeeeefeedebcbcbbcc", sample(0).getSax)
    assertEquals("H4sIAAAAAAAAAFWWbUgUURSGdRVMSVKT8oeRQZCW0opCilZbWFA/+qAgoYI+NIwg+hEhBCVlpQa5pe3s7Mzk9gEJCUkUKWWYFhlUSBhUiJQsEWVUpBUk1c4sc569A8LZ43vf+77nnvuRllqwz/72aL609Lx317MKQnr0Kw740lJj4fugHT5/Fv2SnbDE+TZLOBiWsEO3wwT7GwX7g2x+QMgaQpLtCgj2F7ONaBLuckJH6ECbYP8FhKEBwAey15g4LyCAMQD+oIQ1zDaN9M8Buyy/O7JiIF/0P+kZdpVidmEqhf+nJrVbgLFlmoQaRZjQBVsOmRfdPlVAmSrgz3kB9gRZMbS8pC6LDQE0Y9uLrJuaZCMsf5/mCnBANzRXQKzYlOo4w8MYTMbKbRSu02XYVcIKlsAL9iJa5gXFzTgWvlHDV2hI0IWsi57KIOtDehtFuAq2iXUsJlyFyIVgc9GwHGX3kb4bQxYL3XlRDK1nWB7YKb8vzeMtdEEHGZpL3U6qS7Rdd5co1oNY9rMYEQQXAejX6eIOwc4AkI20ywg+TNn7QwK4o7uynKHhkCvLAdajJYPO/MKhsMOU7BpTBDQayn7INxTSOoYPmTBRqjam6gOQY7mkjvVzptLjiwH2MPyNJZoOIW+rxaFmKva7TUXpQoBhg56E6ZEpSzHHRD+ASYNjDDI/gGG1VNWWK8D5b/4lId1riauncUWh/kUImA0giO4qVr2cYQ+plhVXIhgihlL3Y6ZyuG1kTDsGJ8lGKNFPwrdxC2NIdvUlyZZanCIYWwp2jPAEDTUcEoYhZhuzhGFRNJvkDSfKpQVqnCmvYL+K2f9Qywnpm9iOUDs8G9LTVKIQ0k1kK+FvJes1uJMI54OdpSvHyRa1cUcpzouQhBtoxhrCTjbeElNpxsOqq0bsF8J/ISTD5wLIxqsXAVfotVMMq4BsbVyJAJzF9jYOpGHq8pUT0WPY63sr0Z0nE6OvkVdKGGH2tzTPBKJv0V11DBvFymOyfvZVOQwP4J1Gfzf6OxDZTgUKOdHLEDnuGOxMdH+3sKunQkpPPDGUnbqSy6QXS93cCk3IrEb83+iwFJvU25MYf+BugKMM+UGW6ih91cotNMKFnoKgZp5gKfjZSfZMO/uB220/vHHvgFp8NAGYyWyZQfu+rnQdDvD8+sSM93jGrGDGWtbkI++REvrvCC+PmrhnDNlOwjpqP91mS8pxCZM0++f26F9CdLGfyGlVisvv7XYftHjiN2x9ULk9uhwWj0t617G91P15gDI1YGVQt2m/y/7p1ZR3ynLdl5HwHzsmJESPDAAA", sample(0).getValueAsString)

    assertEquals(288, sample(0).getCount)
    assertEquals(1651.561111111111, sample(0).getAvg)
    assertEquals(178.49958904923878, sample(0).getStd_dev)
    assertEquals(68.8, sample(0).getMin)
    assertEquals(2145.6, sample(0).getMax)
    assertEquals(1496.6, sample(0).getFirst)
    assertEquals(1615.4, sample(0).getLast)


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

 // @Test
  def testChunksV0Guess() = {


     //simply recompute sax string's new parameters, for name = ack and day =2019-11-29 we have 32.metric_id
    val sample = it4MetricsChunksDS
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .withColumn("guess", guess( $"values"))
      //.as[ChunkRecordV0]
      .collect()
    logger.debug(sample(0).toString)
    assertEquals("[20, 3, 2, 1.0649860288788477, 249, 15, 249, 274, 4, 0.9722222222222222, 249, 0.26666666666666666, 12]", sample(0)(16))
  }

 // @Test
  def testChunksV0Sax_best_guess() = {


  //simply recompute sax string with it's new parameters, for name = ack and day =2019-11-29 we have 32.metric_id

    val sample1 = it4MetricsChunksDS
      .where("name = 'ack' AND day = '2019-11-29'")
      .withColumn("guess", guess( $"values"))
      .withColumn("sax_best_guess", sax_best_guess( lit(0.05),  $"values"))
      .withColumn("sax_best_paa_fixed", sax_best_guess_paa_fixed( lit(0.05),lit(50),  $"values"))
     // .withColumn("anomaly_test", anomalie_test($"sax_best_guess"))
      //.agg(count($"tags.metric_id").as("count_metric"))
      //.as[ChunkRecordV0]
      .drop("chunk", "timestamps","values","tags")
     // .collect()

    //assertEquals("adebdcdcddbddddccfdfegfffghigiihhgggifghffcfgfffff", sample1(2)(16))

  }
  //@Test
  def testChunksV0Sax_anomaly() = {


             //simply check for anomalies in the sax string with it's new parameters, for name = ack and day =2019-11-29 we have 32.metric_id
            val sample = it4MetricsChunksDS
              .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
              .withColumn("sax_best_guess", sax_best_guess( lit(0.01),lit(50),  $"values"))
              .withColumn("anomaly_test", anomalie_test($"sax_best_guess"))
              //.as[ChunkRecordV0]
              .collect()
            logger.debug(sample(0).toString)
            assertEquals("[15, 41]", sample(0)(17))
  }

}
