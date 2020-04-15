package com.hurence.historian.model


/**
  * For
  */
sealed trait Measure {
  def name: String

  def value: Double

  def timestamp: Long

  def lat: Option[Double] = None

  def lon: Option[Double] = None
}


case class MeasureRecordV0(name: String,
                           value: Double,
                           timestamp: Long,
                           year: Int,
                           month: Int,
                           day: String,
                           hour: Int,
                           tags: List[String],
                           override val lat: Option[Double] = None,
                           override val lon: Option[Double] = None) extends Measure


