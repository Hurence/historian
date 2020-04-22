package com.hurence.historian.spark.sql.reader

object ReaderType extends Enumeration {
  type ReaderType = Value

  val EVOA_CSV_MEASURES_READER:Value  = Value("EVOA_CSV_MEASURES_READER")
  val ITDATA_CSV_MEASURES_READER:Value = Value("ITDATA_CSV_MEASURES_READER")
  val PARQUET_MEASURES_READER:Value = Value("PARQUET_MEASURES_READER")
  val PARQUET_CHUNKS_READER:Value = Value("PARQUET_CHUNKS_READER")
}