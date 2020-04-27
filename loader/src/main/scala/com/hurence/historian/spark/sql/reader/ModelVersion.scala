package com.hurence.historian.spark.sql.reader


object ModelVersion extends Enumeration {
  type ModelVersion = Value

  val V0:Value  = Value("V0")
  val EVOA_V0:Value = Value("EVOA_V0")
}