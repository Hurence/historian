package com.hurence.historian.spark.sql.reader

import com.hurence.historian.spark.sql.reader.ReaderType.ReaderType
import com.hurence.historian.spark.sql.reader.csv.{EvoaCSVTimeseriesReader, RDBoosterCSVTimeseriesReader}
import com.hurence.historian.spark.sql.reader.parquet.ParquetTimeseriesReader





object TimeseriesReaderFactory {


  def apply(readerType: ReaderType): TimeseriesReader = readerType match {
    case ReaderType.EVOA_CSV => new EvoaCSVTimeseriesReader()
    case ReaderType.RD_BOOSTER_CSV => new RDBoosterCSVTimeseriesReader()
    case ReaderType.PARQUET => new ParquetTimeseriesReader()
  }
}