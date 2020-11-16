package com.hurence.historian.spark.sql.reader

import com.hurence.historian.spark.sql.reader.ChunksReaderType.ChunksReaderType
import com.hurence.historian.spark.sql.reader.MeasuresReaderType.MeasuresReaderType
import com.hurence.historian.spark.sql.reader.csv.{EvoaCSVMeasuresReader, GenericMeasuresReaderV0, GenericMeasuresStreamReader, ITDataCSVMeasuresReaderV0}
import com.hurence.historian.spark.sql.reader.parquet.{ParquetChunksReader, ParquetMeasuresReader}
import com.hurence.historian.spark.sql.reader.solr.SolrChunksReader
import com.hurence.timeseries.model.{Chunk, Measure}


/**
  * Instanciate a Reader for the given type
  */
object ReaderFactory {


  def getMeasuresReader(readerType: MeasuresReaderType): Reader[Measure] = readerType match {
    case MeasuresReaderType.EVOA_CSV => new EvoaCSVMeasuresReader()
    case MeasuresReaderType.ITDATA_CSV => new ITDataCSVMeasuresReaderV0()
    case MeasuresReaderType.PARQUET => new ParquetMeasuresReader()
    case MeasuresReaderType.GENERIC_CSV => new GenericMeasuresReaderV0()
    case MeasuresReaderType.GENERIC_STREAM_CSV => new GenericMeasuresStreamReader()
  }

  def getChunksReader(readerType: ChunksReaderType): Reader[Chunk] = readerType match {
    case ChunksReaderType.PARQUET => new ParquetChunksReader()
    case ChunksReaderType.SOLR => new SolrChunksReader()
  }
}