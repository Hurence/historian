package com.hurence.historian.spark.sql.reader

import com.hurence.historian.spark.sql.Options
import org.apache.spark.sql.Dataset

trait Reader[T] {
  def read(options: Options): Dataset[_ <: T]
}
