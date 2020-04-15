package com.hurence.historian.model

import java.util.{Calendar, Date}

import com.hurence.historian.model.MeasureRecordV0
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Calendar

class MeasureTest {

  @Test
  def testMeasureV0() = {

    val cal = Calendar.getInstance

    val measureName = "cpu"
    val measureValue = 76.4d
    val measureTimestamp = new Date().getTime
    val measureYear = cal.get(Calendar.YEAR)
    val measureMonth = cal.get(Calendar.MONTH)
    val measureDay = cal.get(Calendar.DAY_OF_MONTH)
    val measureLat = 44.75420d
    val measureLon = 5.373523d
    val tags = List("host:host1", "ip:12.1.1.1")

    val measure = MeasureRecordV0(measureName, measureValue, measureTimestamp, measureYear, measureMonth, "2020-15-04", 2, tags, lon = Some(measureLon))

    assertEquals(measureName, measure.name)
    assertEquals(measureValue, measure.value)
    assertEquals(measureTimestamp, measure.timestamp)
    assertEquals(measureYear, measure.year)
    assertEquals(tags, measure.tags)
    assertEquals(false, measure.lat.isDefined)
    assertEquals(measureLon, measure.lon.get)

  }
}
