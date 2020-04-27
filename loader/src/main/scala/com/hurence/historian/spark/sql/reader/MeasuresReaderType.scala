package com.hurence.historian.spark.sql.reader

object MeasuresReaderType extends Enumeration {
  type MeasuresReaderType = Value

  val EVOA_CSV:Value  = Value("EVOA_CSV_MEASURES_READER")
  val ITDATA_CSV:Value = Value("ITDATA_CSV_MEASURES_READER")
  val PARQUET:Value = Value("PARQUET_MEASURES_READER")
}