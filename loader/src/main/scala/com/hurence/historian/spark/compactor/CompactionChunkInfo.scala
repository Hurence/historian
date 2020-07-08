package com.hurence.historian.spark.compactor

import com.hurence.timeseries.modele.PointImpl

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long) {

  def getPoints(): List[PointImpl] = {
    //TODO
//    TimeSeriesRecord.getPointStream(value, start, end).asScala.toList
    null
  }
}
