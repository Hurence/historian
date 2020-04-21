package com.hurence.historian.model

import java.util.Date
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
    val measureLat = "44.75420"
    val measureLon = "5.373523"
    val tags = Map("host" -> "host1", "ip" -> "12.1.1.1", "lon" -> measureLon)

    val measure = MeasureRecordV0(measureName, measureValue, measureTimestamp, measureYear, measureMonth, "2020-15-04", 2, tags)

    assertEquals(measureName, measure.name)
    assertEquals(measureValue, measure.value)
    assertEquals(measureTimestamp, measure.timestamp)
    assertEquals(measureYear, measure.year)
    assertEquals(tags, measure.tags)
    assertEquals(measureLon, measure.tags("lon"))

  }
}
