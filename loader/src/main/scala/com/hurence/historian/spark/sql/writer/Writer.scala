package com.hurence.historian.spark.sql.writer

import com.hurence.historian.spark.sql.Options
import org.apache.spark.sql.Dataset

trait Writer[T] {

  def write(options: Options, ds: Dataset[_ <: T])

}
