package com.hurence.historian.spark.sql.writer.parquet

import com.hurence.historian.LoaderOptions
import com.hurence.historian.spark.sql.writer.TimeseriesWriter
import com.hurence.logisland.record.TimeseriesRecord
import org.apache.spark.sql.Dataset

class ParquetTimeseriesWriter extends TimeseriesWriter {


  override def write(options: LoaderOptions, ds: Dataset[TimeseriesRecord]) = {

    ds
      .write
      .partitionBy("day")
      .mode("append")
      .parquet(options.out)
  }
}
