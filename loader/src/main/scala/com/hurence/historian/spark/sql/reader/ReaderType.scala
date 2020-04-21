package com.hurence.historian.spark.sql.reader

object ReaderType extends Enumeration {
  type ReaderType = Value

  val EVOA_CSV:Value  = Value("EVOA_CSV")
  val RD_BOOSTER_CSV:Value = Value("RD_BOOSTER_CSV")
  val PARQUET:Value = Value("PARQUET")
}