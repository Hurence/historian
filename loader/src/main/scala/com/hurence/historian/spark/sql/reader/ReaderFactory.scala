package com.hurence.historian.spark.sql.reader

import com.hurence.historian.model.HistorianRecord
import com.hurence.historian.spark.sql.reader.ReaderType.ReaderType
import com.hurence.historian.spark.sql.reader.csv.{EvoaCSVMeasuresReader, ITDataCSVMeasuresReaderV0}
import com.hurence.historian.spark.sql.reader.parquet.{ParquetChunksReader, ParquetMeasuresReader}


/**
  * Instanciate a Reader for the given type
  */
object ReaderFactory {


  def apply(readerType: ReaderType): Reader[_ <: HistorianRecord] = readerType match {
    case ReaderType.EVOA_CSV_MEASURES_READER => new EvoaCSVMeasuresReader()
    case ReaderType.ITDATA_CSV_MEASURES_READER => new ITDataCSVMeasuresReaderV0()
    case ReaderType.PARQUET_MEASURES_READER => new ParquetMeasuresReader()
    case ReaderType.PARQUET_CHUNKS_READER => new ParquetChunksReader()
  }
}


class ReaderFactory[T] {

}