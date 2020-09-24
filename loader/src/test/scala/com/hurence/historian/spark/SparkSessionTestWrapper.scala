package com.hurence.historian.spark

import com.hurence.historian.spark.common.Definitions._
import com.hurence.historian.spark.sql.functions.sax
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.model.{Chunk, Measure}
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.collection.JavaConverters._


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
    * *
    * +----+------+-------------+----+-----+----+--------------------+----------+
    * |name| value|    timestamp|year|month|hour|                tags|       day|
    * +----+------+-------------+----+-----+----+--------------------+----------+
    * | ack| 142.2|1574895600000|2019|   11|   0|[metric_id -> b77...|2019-11-28|
    * | ack|   0.0|1574895609000|2019|   11|   0|[metric_id -> 1d0...|2019-11-28|
    *
    */
  lazy val it4MetricsDS = {
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath

    implicit val measureEncoder = Encoders.bean(classOf[Measure])

    spark.read
      .parquet(filePath)
      .map(r => Measure.builder()
        .name(r.getAs[String]("name"))
        .timestamp(r.getAs[Long]("timestamp"))
        .value(r.getAs[Double]("value"))
        .tags(r.getAs[Map[String, String]]("tags").asJava)
        .compute()
        .build()
      ).as[Measure]
      .cache()
  }


  /**
    *
    *
    *
    * +--------------+----------+-------------+-------------+--------------------+--------------------+-----+------------------+-------------------+-------+---------+--------+-------+--------------------+--------------------+--------------------+
    * |          name|       day|        start|          end|          timestamps|              values|count|               avg|             stddev|    min|      max|   first|   last|                 sax|                tags|               chunk|
    * +--------------+----------+-------------+-------------+--------------------+--------------------+-----+------------------+-------------------+-------+---------+--------+-------+--------------------+--------------------+--------------------+
    * |           ack|2019-11-26|1574722966000|1574809058000|[1574722966000, 1...|[1426.2, 1558.6, ...|  288|1647.3284722222215|  112.0923848462279| 1300.8|   2251.2|  1426.2| 1525.6|cdbbacbccccbbbdcd...|[metric_id -> 2e4...|H4sIAAAAAAAAAFWWa...|
    * |           ack|2019-11-30|1575068475000|1575154567000|[1575068475000, 1...|[149.8, 147.2, 13...|  288|144.09097222222226| 15.833389556107775|   95.6|    193.0|   149.8|  143.2|dffgffefeefedcfee...|[metric_id -> dea...|H4sIAAAAAAAAAFVWP...|
    * |     cpu_ready|2019-11-29|1574982139000|1575068228000|[1574982139000, 1...|[4.0, 8.0, 19.0, ...|  288| 6.645833333333333|  4.183039839596509|    2.0|     37.0|     4.0|   10.0|ebgbcbcbbcbbcbbbb...|[metric_id -> 79e...|H4sIAAAAAAAAAIVTz...|
    * |memoryConsumed|2019-11-30|1575068705000|1575154629000|[1575068705000, 1...|[24872.0, 24872.0...|  125|         30486.016|  7243.418238230863|18992.0|  39824.0| 24872.0|24872.0|bbbbbabbbbbbbbbbb...|[metric_id -> 2c1...|H4sIAAAAAAAAAOPi1...|
    *
    *
    *
    */


  lazy val it4MetricsChunksDS = {
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath


    implicit val chunkEncoder = Encoders.bean(classOf[Chunk])
    val ds = spark.read
      .parquet(filePath)
      .filter(r => r.getAs[String]("chunk") != null)
      .map(r => {

        Chunk.builder()
          .name(r.getAs[String]("name"))
          .start(r.getAs[Long]("start"))
          .end(r.getAs[Long]("end"))
          .count(r.getAs[Long]("count"))
          .avg(r.getAs[Double]("avg"))
          .stdDev(r.getAs[Double]("std_dev"))
          .min(r.getAs[Double]("min"))
          .max(r.getAs[Double]("max"))
          .first(r.getAs[Double]("first"))
          .last(r.getAs[Double]("last"))
          .sax(r.getAs[String]("sax"))
          .value(BinaryEncodingUtils.decode(r.getAs[String](CHUNK_COLUMN)))
          .tags(r.getAs[Map[String, String]]("tags").asJava)
          .buildId()
          .computeMetrics()
          .build()


      }).as[Chunk]
      .cache()

    ds

  }
}