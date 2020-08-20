package com.hurence.historian.spark.sql

import java.util.Date

import com.hurence.historian.modele.ChunkRecordV0
import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql.transformer.{Rechunkifyer, RechunkifyerOptions}
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.slf4j.LoggerFactory


class RechunkifyerTest extends SparkSessionTestWrapper {

  private val logger = LoggerFactory.getLogger(classOf[LoaderTests])
  import spark.implicits._

  @Test
  def TestRechunkifyer {

    val now = new Date().getTime
    val oneSec = 1000L
    val oneMin = 60 * oneSec
    val oneHour = 60 * oneMin
    val oneDay = 24 * oneHour


    val dataFrame = spark.createDataFrame(Seq(
      ("metric_a", 1.3, now + 30 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.2, now + 20 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.1, now + 10 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.4, now + 40 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.5, now + 50 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.6, now + oneHour, Map("metric_id" -> 1)),
      ("metric_a", 1.7, now + oneDay + 10 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.8, now + oneDay + 20 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 2.1, now + 10 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.2, now + 20 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.3, now + 30 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.4, now + oneHour, Map("metric_id" -> 2)),
      ("metric_b", 3.1, now + 10 * oneMin, Map("metric_id" -> 3)),
      ("metric_b", 3.2, now + 20 * oneMin, Map("metric_id" -> 3))
    ))
      .toDF("name", "value", "timestamp", "tags")
//    dataFrame.show()
    val chunkyfier = new Chunkyfier()
      .setValueCol("value")
      .setTimestampCol("timestamp")
      .setChunkCol("chunk")
      .setGroupByCols(Array("name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .setChunkMaxSize(1440)
      .setSaxAlphabetSize(4)
      .setSaxStringLength(4)


    val bucketedData = chunkyfier.transform(dataFrame).as[ChunkRecordV0]
    val bucketedDataCollect = bucketedData.collect()


    logger.debug(bucketedDataCollect(3).toString)
    assertEquals("metric_a", bucketedDataCollect(3).name)
    assertEquals("2", bucketedDataCollect(3).tags("metric_id"))
    assertEquals(4, bucketedDataCollect(3).count)
    assertEquals(2.1, bucketedDataCollect(3).min)
    assertEquals(2.4, bucketedDataCollect(3).max)
    assertEquals(2.1, bucketedDataCollect(3).first)
    assertEquals(2.4, bucketedDataCollect(3).last)
    assertEquals(0.12909944487358033, bucketedDataCollect(3).stddev)
    assertEquals(2.25, bucketedDataCollect(3).avg)
    assertEquals("H4sIAAAAAAAAAOPi1Dx7BgQYHLh4FQ6cV9GcNRMEGB24ODXTwIAJKCNw4F2upjEYMDsIMAAACfULTjYAAAA=", bucketedDataCollect(3).chunk)

    val options = new RechunkifyerOptions
    val rechunkify = new Rechunkifyer

    val newBucketedData = rechunkify.transform(options, bucketedData)
//    newBucketedData.printSchema()
//    newBucketedData.show(1,false)

    val newBucketedDataChunkRecord = newBucketedData.as[ChunkRecordV0].collect()
//    println(newBucketedDataChunkRecord(0))


    logger.debug(newBucketedDataChunkRecord(3).toString)
    assertEquals("metric_a", newBucketedDataChunkRecord(3).name)
    assertEquals("2", newBucketedDataChunkRecord(3).tags("metric_id"))
    assertEquals(4, newBucketedDataChunkRecord(3).count)
    assertEquals(2.1, newBucketedDataChunkRecord(3).min)
    assertEquals(2.4, newBucketedDataChunkRecord(3).max)
    assertEquals(2.1, newBucketedDataChunkRecord(3).first)
    assertEquals(2.4, newBucketedDataChunkRecord(3).last)
    assertEquals(0.12909944487358033, newBucketedDataChunkRecord(3).stddev)
    assertEquals(2.25, newBucketedDataChunkRecord(3).avg)
    assertEquals("H4sIAAAAAAAAAOPi1Dx7BgQYHLh4FQ6cV9GcNRMEGB24ODXTwIAJKCNw4F2upjEYMDsIMAAACfULTjYAAAA=", newBucketedDataChunkRecord(3).chunk)

  spark.close()
  }
}
