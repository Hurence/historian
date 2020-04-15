package com.hurence.historian.spark.sql.writer

import com.hurence.historian.LoaderOptions
import com.hurence.logisland.record.TimeseriesRecord
import org.apache.spark.sql.Dataset

trait TimeseriesWriter {

  def write(options: LoaderOptions, ds: Dataset[TimeseriesRecord])
}
