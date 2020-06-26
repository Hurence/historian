package com.hurence.historian.spark.compactor

import com.hurence.logisland.record.TimeSeriesRecord
import com.hurence.timeseries.modele.Point

import scala.collection.JavaConverters._

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long) {

  def getPoints(): List[Point] = {
    TimeSeriesRecord.getPointStream(value, start, end).asScala.toList
  }
}
