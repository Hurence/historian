package com.hurence.historian.spark.sql.writer

import com.hurence.historian.spark.sql.writer.WriterType.WriterType
import com.hurence.historian.spark.sql.writer.parquet.ParquetTimeseriesWriter
import com.hurence.historian.spark.sql.writer.solr.SolrTimeseriesWriter


/**
  * Timeseries Reader factory to get the one you need to load dataframe
  */
object TimeseriesWriterFactory {


  def apply(writerType: WriterType): TimeseriesWriter = writerType match {
    case WriterType.PARQUET => new ParquetTimeseriesWriter()
    case WriterType.SOLR => new SolrTimeseriesWriter()
  }
}