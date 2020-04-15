package com.hurence.historian.spark.compactor

import com.hurence.logisland.record.{Point, TimeseriesRecord}
import scala.collection.JavaConverters._

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long) {

  def getPoints(): List[Point] = {
    TimeseriesRecord.getPointStream(value, start, end).asScala.toList
  }
}
