package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.LoaderOptions
import com.hurence.historian.model.MeasureRecordV0
import com.hurence.historian.spark.sql.reader.TimeseriesReader
import com.hurence.logisland.record.TimeseriesRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class ParquetTimeseriesReader extends TimeseriesReader {


  override def read(options: LoaderOptions): Dataset[MeasureRecordV0] = {


    val spark = SparkSession.getActiveSession.get

    import spark.implicits._
    implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeseriesRecord]

    spark.read.parquet(options.in)
      .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .as[MeasureRecordV0]
  }
}
