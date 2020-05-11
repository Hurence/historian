package com.hurence.historian.spark.sql.reader

import com.hurence.historian.spark.sql.reader.ChunksReaderType.ChunksReaderType
import com.hurence.historian.spark.sql.reader.MeasuresReaderType.MeasuresReaderType
import com.hurence.historian.spark.sql.reader.csv.{EvoaCSVMeasuresReader, GenericMeasuresReaderV0, ITDataCSVMeasuresReaderV0}
import com.hurence.historian.spark.sql.reader.parquet.{ParquetChunksReader, ParquetMeasuresReader}
import com.hurence.historian.spark.sql.reader.solr.SolrChunksReader


/**
  * Instanciate a Reader for the given type
  */
object ReaderFactory {


  def getMeasuresReader(readerType: MeasuresReaderType)  = readerType match {
    case MeasuresReaderType.EVOA_CSV => new EvoaCSVMeasuresReader()
    case MeasuresReaderType.ITDATA_CSV => new ITDataCSVMeasuresReaderV0()
    case MeasuresReaderType.PARQUET => new ParquetMeasuresReader()
    case MeasuresReaderType.GENERIC_CSV => new GenericMeasuresReaderV0()
  }

  def getChunksReader(readerType: ChunksReaderType)  = readerType match {
    case ChunksReaderType.PARQUET => new ParquetChunksReader()
    case ChunksReaderType.SOLR => new SolrChunksReader()
  }
}