package com.hurence.historian.spark.sql.reader

import org.apache.spark.sql.Dataset

trait Reader[T] {
  def read(options: ReaderOptions): Dataset[T]
}
