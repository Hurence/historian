package com.hurence.historian

import com.hurence.logisland.record.{Point, TimeSeriesRecord}
import collection.JavaConverters._

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long) {

  def getPoints(): List[Point] = {
    TimeSeriesRecord.getPointStream(value, start, end).asScala.toList
  }
}
