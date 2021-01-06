package com.hurence.historian.spark.ml

import com.hurence.historian.date.util.DateUtil
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{Test, TestInstance}
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
