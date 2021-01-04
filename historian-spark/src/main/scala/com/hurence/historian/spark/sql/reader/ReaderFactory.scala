package com.hurence.historian.spark.sql.reader

import com.hurence.historian.spark.sql.reader.ReaderType.ReaderType
import com.hurence.historian.spark.sql.reader.csv._
import com.hurence.historian.spark.sql.reader.parquet._
import com.hurence.historian.spark.sql.reader.solr._
import com.hurence.timeseries.model.{Chunk, Measure}


/**
  * Instanciate a Reader for the given type
  */
object ReaderFactory {

  def getMeasuresReader(readerType: ReaderType): Reader[Measure] = readerType match {
    case ReaderType.PARQUET => new ParquetMeasuresReader()
    case ReaderType.CSV => new CsvMeasuresReader()
    case ReaderType.STREAM_CSV => new CsvMeasuresStreamReader()
  }

  def getChunksReader(readerType: ReaderType): Reader[Chunk] = readerType match {
    case ReaderType.PARQUET => new ParquetChunksReader()
    case ReaderType.SOLR => new SolrChunksReader()
  }
}