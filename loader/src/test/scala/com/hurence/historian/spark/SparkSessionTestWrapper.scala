package com.hurence.historian.spark


import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * Initialize a Spark session for unit tests
  */
trait SparkSessionTestWrapper {

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

    implicit val measureEncoder = Encoders.bean(classOf[Measure])

    spark.read
      .parquet(filePath)
      .as[Measure]
      .cache()
  }

}