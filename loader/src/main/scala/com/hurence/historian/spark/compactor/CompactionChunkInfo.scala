package com.hurence.historian.spark.compactor

import com.hurence.logisland.record.{Point, TimeSeriesRecord}
import scala.collection.JavaConverters._

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long) {

  def getPoints(): List[Point] = {
    TimeSeriesRecord.getPointStream(value, start, end).asScala.toList
  }
}
