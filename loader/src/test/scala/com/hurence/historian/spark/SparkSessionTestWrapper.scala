package com.hurence.historian.spark

import com.hurence.historian.model.{ChunkRecordV0, MeasureRecordV0}
import org.apache.spark.sql.SparkSession

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
