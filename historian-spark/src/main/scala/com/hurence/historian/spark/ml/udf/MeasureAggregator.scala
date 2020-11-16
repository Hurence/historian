package com.hurence.historian.spark.ml.udf


import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator



class MeasureAggregator extends Aggregator[Measure, List[Long], List[Long]] {
  override def zero: List[Long] = List()

  override def reduce(b: List[Long], m: Measure): List[Long] = {
      m.getTimestamp :: b
  }

  override def merge(b1: List[Long], b2: List[Long]): List[Long] = {
    if (b1 != null && b2 != null) {
      b2 ::: b1
    } else if (b1 != null && b2 == null) {
      b1
    } else
      b2
  }

  override def finish(reduction: List[Long]): List[Long] = reduction

  override def bufferEncoder: Encoder[List[Long]] = {
    Encoders.bean(classOf[List[Long]])
  }

  override def outputEncoder: Encoder[List[Long]] = {
    Encoders.bean(classOf[List[Long]])
  }
}
