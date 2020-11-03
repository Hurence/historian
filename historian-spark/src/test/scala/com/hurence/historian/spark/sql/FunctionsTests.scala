package com.hurence.historian.spark.sql

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.hurence.historian.date.util.DateUtil
import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql.functions.toTimestampUTC
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, MeasuresReaderType, ReaderFactory}
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Chunk.MetricKey.{TAG_KEY_VALUE_SEPARATOR_CHAR, TOKEN_SEPARATOR_CHAR}
import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.slf4j.LoggerFactory

@TestInstance(Lifecycle.PER_CLASS)
class FunctionsTests {

  private val logger = LoggerFactory.getLogger(classOf[FunctionsTests])


  @Test
  def testDateUtils() = {

    val utcTime = 1583021174000L
    val dateString = "01/03/2020 00:06:14"
    val dateFormat = "dd/MM/yyyy HH:mm:ss"
    val time = DateUtil.parse(dateString, dateFormat).getTime

    assertEquals(utcTime, time)
  }
}
