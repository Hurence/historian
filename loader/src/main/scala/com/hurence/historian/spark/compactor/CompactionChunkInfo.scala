package com.hurence.historian.spark.compactor

import com.hurence.timeseries.model.Measure

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long) {

  def getPoints(): List[Measure] = {
    //TODO
//    TimeSeriesRecord.getPointStream(value, start, end).asScala.toList
    null
  }
}
