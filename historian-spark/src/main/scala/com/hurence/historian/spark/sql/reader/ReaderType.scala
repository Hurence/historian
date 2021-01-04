package com.hurence.historian.spark.sql.reader

object ReaderType extends Enumeration {
  type ReaderType = Value

  val PARQUET:Value = Value("parquet")
  val SOLR:Value = Value("solr")
  val CSV: Value = Value("csv")
  val STREAM_CSV: Value = Value("stream_csv")
}