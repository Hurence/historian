package com.hurence.historian.spark

import com.hurence.timeseries.modele.chunk.ChunkVersion0Impl
import com.hurence.timeseries.modele.measure.MeasureVersionV0Impl
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


  lazy val it4MetricsDS = {
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath

    spark.read
      .parquet(filePath)
      .as[MeasureVersionV0Impl](Encoders.bean(classOf[MeasureVersionV0Impl]))
      .cache()
  }

  lazy val it4MetricsChunksDS = {
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath

    spark.read
      .parquet(filePath)
      .as[ChunkVersion0Impl](Encoders.bean(classOf[ChunkVersion0Impl]))
      .cache()
  }
}
