package com.hurence.historian.spark

import com.hurence.historian.modele.{ChunkRecordV0, MeasureRecordV0}
import org.apache.spark.sql.SparkSession


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


  lazy val it4MetricsDS = {

    import spark.implicits._
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath

    spark.read
      .parquet(filePath)
      .as[MeasureRecordV0]
      .cache()
  }

  lazy val it4MetricsChunksDS = {

    import spark.implicits._
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath

    spark.read
      .parquet(filePath)
      .as[ChunkRecordV0]
      .cache()
  }
}
