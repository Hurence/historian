package com.hurence.historian.spark


import com.hurence.timeseries.model.Definitions._

import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, DateTimeParser}
import org.joda.time.{DateTime, DateTimeZone}
import collection.JavaConverters._
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
      .map(r => {
        Measure.builder()
          .name(r.getAs[String](FIELD_NAME))
          .timestamp(r.getAs[Long](FIELD_TIMESTAMP))
          .quality(r.getAs[Float](FIELD_QUALITY))
          .value(r.getAs[Double](FIELD_VALUE))
          .tags(r.getAs[Map[String, String]](FIELD_TAGS).asJava)
          .build()
      })
      .as[Measure]
      .cache()
  }

  def randomMeasure(name: String,
                    tags: java.util.Map[String, String],
                    lowerValueBound:Double,
                    upperValueBound:Double,
                    withinDay: String = "",
                    withQuality: Boolean = true): Measure = {

    val newTimestamp = if(withinDay.isEmpty) {
      val time = new DateTime(1977, 3, 2, 2, 13)
      time.getMillis + (Math.random * 1000L).toLong
    }else{
      val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")
      val time = dtf.parseDateTime(withinDay)
      val timePlusOneDay = time.plusDays(1)
       (time.getMillis  + (timePlusOneDay.getMillis - time.getMillis) * Math.random  ).toLong
    }
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