package com.hurence.historian.spark.sql.reader

object ChunksReaderType extends Enumeration {
  type ChunksReaderType = Value

  val PARQUET:Value = Value("PARQUET_CHUNKS_READER")
  val SOLR:Value = Value("SOLR_CHUNKS_READER")
}