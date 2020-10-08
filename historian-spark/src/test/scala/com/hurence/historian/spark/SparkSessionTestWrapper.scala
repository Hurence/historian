package com.hurence.historian.spark


import java.time.ZoneId
import java.util
import java.util.{Map, TimeZone}

import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Initialize a Spark session for unit tests
  */
trait SparkSessionTestWrapper {
  implicit val measureEncoder = Encoders.bean(classOf[Measure])

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }

  /**
    * +----+------+-------------+----+-----+----+--------------------+----------+-------+
    * |name| value|    timestamp|year|month|hour|                tags|       day|quality|
    * +----+------+-------------+----+-----+----+--------------------+----------+-------+
    * | ack| 142.2|1574895600000|2019|   11|   0|[metric_id -> b77...|2019-11-28|  143.2|
    * | ack|   0.0|1574895609000|2019|   11|   0|[metric_id -> 1d0...|2019-11-28|    1.0|
    * | ack|   0.0|1574895613000|2019|   11|   0|[metric_id -> 639...|2019-11-28|    1.0|
    */
  lazy val it4MetricsDS = {
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath

    spark.read
      .parquet(filePath)
      .as[Measure]
      .cache()
  }

  def randomMeasure(name: String,
                    tags: util.Map[String, String],
                    lowerValueBound:Double,
                    upperValueBound:Double,
                    withQuality: Boolean = true): Measure = {
    val time: DateTime = new DateTime(1977, 3, 2, 2, 13)
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC"))))
    val newTimestamp: Long = time.getMillis + (Math.random * 1000L).toLong
    val newValue = Math.random * (upperValueBound - lowerValueBound) + lowerValueBound
    val newQuality = if(withQuality) Math.random.toFloat else Float.NaN

    Measure.builder
      .name(name)
      .value(newValue)
      .quality(newQuality)
      .timestamp(newTimestamp)
      .tags(tags)
      .build
  }

  lazy val randomMetricsDS = {


  }

}