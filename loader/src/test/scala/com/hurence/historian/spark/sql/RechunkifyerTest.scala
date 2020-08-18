package com.hurence.historian.spark.sql

import java.util.Date

import com.hurence.historian.modele.ChunkRecordV0
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql.transformer.{Rechunkifyer, RechunkifyerOptions}
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.slf4j.LoggerFactory


class RechunkifyerTest {
private val logger = LoggerFactory.getLogger(classOf[LoaderTests])
  @Test
  def TestRechunkifyer() {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ReChunkyfierExample")
      .getOrCreate()
    import spark.implicits._

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


    val options = new RechunkifyerOptions
    val rechunkify = new Rechunkifyer

    val newBucketedData = rechunkify.transform(options, bucketedData)
//    newBucketedData.printSchema()
//    newBucketedData.show(1,false)

    val newBucketedDataChunkRecord = newBucketedData.as[ChunkRecordV0].collect()
//    println(newBucketedDataChunkRecord(0))

    logger.debug(newBucketedDataChunkRecord(0).toString)
    assertEquals("metric_a", newBucketedDataChunkRecord(0).name)
    assertEquals("1", newBucketedDataChunkRecord(0).tags("metric_id"))
    assertEquals(2, newBucketedDataChunkRecord(0).count)
    assertEquals(1.7, newBucketedDataChunkRecord(0).min)
    assertEquals(1.8, newBucketedDataChunkRecord(0).max)
    assertEquals(1.7, newBucketedDataChunkRecord(0).first)
    assertEquals(1.8, newBucketedDataChunkRecord(0).last)
    assertEquals(0.07071067811865482, newBucketedDataChunkRecord(0).stddev)
    assertEquals(1.75, newBucketedDataChunkRecord(0).avg)
    assertEquals("H4sIAAAAAAAAAOPi1DQGg9/2XLwCB86raJ49AwJ/7AUYAMo5bj4cAAAA", newBucketedDataChunkRecord(0).chunk)


  }
}
