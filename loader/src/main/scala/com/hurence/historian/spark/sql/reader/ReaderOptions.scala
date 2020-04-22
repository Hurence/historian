package com.hurence.historian.spark.sql.reader

import com.hurence.historian.spark.sql.reader.ReaderType.ReaderType


case class ReaderOptions( readerType: ReaderType,
                          in: String,
                          useKerberos: Boolean = false)
