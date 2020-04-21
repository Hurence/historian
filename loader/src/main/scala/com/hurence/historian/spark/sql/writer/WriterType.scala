package com.hurence.historian.spark.sql.writer

object WriterType extends Enumeration {
  type WriterType = Value

  val SOLR:Value = Value("SOLR")
  val PARQUET:Value = Value("PARQUET")
}