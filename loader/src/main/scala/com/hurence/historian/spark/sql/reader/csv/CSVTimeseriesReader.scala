package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.spark.sql.reader.TimeseriesReader

trait CSVTimeseriesReader  extends TimeseriesReader {


  def config(): Map[String, String]
}
