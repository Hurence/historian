package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.modele.measure.MeasureVersionV0
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

class ParquetMeasuresReader extends Reader[MeasureVersionV0] {


  override def read(options: Options): Dataset[MeasureVersionV0] = {


    val spark = SparkSession.getActiveSession.get

    import spark.implicits._


    spark.read
      .parquet(options.path)
  //    .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .as[MeasureVersionV0](Encoders.bean(classOf[MeasureVersionV0]))
  }
}
