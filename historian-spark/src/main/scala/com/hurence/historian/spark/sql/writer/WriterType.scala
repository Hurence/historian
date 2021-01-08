package com.hurence.historian.spark.sql.writer

object WriterType extends Enumeration {
  type WriterType = Value

  val CSV: Value = Value("csv")
  val SOLR:Value = Value("solr")
  val PARQUET:Value = Value("parquet")
}