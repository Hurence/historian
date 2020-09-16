package com.hurence.historian.model

import java.util.Date

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Calendar

import com.hurence.timeseries.model.Measure

class MeasureTest {

 /* @Test
  def testMeasureV0() = {

    val cal = Calendar.getInstance

    val measureName = "cpu"
    val measureValue = 76.4d
    val measureTimestamp = new Date().getTime
    val measureYear = cal.get(Calendar.YEAR)
    val measureMonth = cal.get(Calendar.MONTH)
    val measureLon = "5.373523"
    val tags = Map("host" -> "host1", "ip" -> "12.1.1.1", "lon" -> measureLon)

    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    val measure = new Measure(measureName, measureValue, measureTimestamp, measureYear, measureMonth, "2020-15-04", tags)

    assertEquals(measureName, measure.getName)
    assertEquals(measureValue, measure.getValue)
    assertEquals(measureTimestamp, measure.getTimestamp)
    assertEquals(measureYear, measure.getYear)
    assertEquals(tags.asJava, measure.getTags)
    assertEquals(measureLon, measure.getTags.get("lon"))

  }*/
}
