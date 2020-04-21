package com.hurence.historian.spark.sql.reader

import com.hurence.historian.LoaderOptions
import com.hurence.historian.model.MeasureRecordV0
import org.apache.spark.sql.Dataset

trait TimeseriesReader {


  def read(options: LoaderOptions): Dataset[MeasureRecordV0]
}
