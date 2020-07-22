package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.modele.MeasureRecordV0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import org.apache.spark.sql.{Dataset, SparkSession}

class ParquetMeasuresReader extends Reader[MeasureRecordV0] {


  override def read(options: Options): Dataset[MeasureRecordV0] = {


    val spark = SparkSession.getActiveSession.get

    import spark.implicits._


    spark.read
      .parquet(options.path)
  //    .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .as[MeasureRecordV0]
  }
}
