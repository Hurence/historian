package com.hurence.historian.spark.sql.writer

import com.hurence.historian.spark.sql.writer.WriterType.WriterType
import com.hurence.historian.spark.sql.writer.parquet.ParquetChunksWriter
import com.hurence.historian.spark.sql.writer.solr.SolrChunksWriter


/**
  * Timeseries Reader factory to get the one you need to load dataframe
  */
object WriterFactory {


  def getChunksWriter(writerType: WriterType) = writerType match {
    case WriterType.PARQUET => new ParquetChunksWriter()
    case WriterType.SOLR => new SolrChunksWriter()
  }

}