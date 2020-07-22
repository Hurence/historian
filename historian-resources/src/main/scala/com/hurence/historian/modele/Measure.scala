package com.hurence.historian.modele

/**
  * A measure is a chronodated value of something
  */
sealed trait Measure extends HistorianRecord {

  def name: String

  def value: Double

  def timestamp: Long

}

/**
  * MeasureRecord V0
  *
  * @param name
  * @param value
  * @param timestamp
  * @param year
  * @param month
  * @param day
  * @param hour
  * @param tags
  */
case class MeasureRecordV0(name: String,
                           value: Double,
                           timestamp: Long,
                           year: Int,
                           month: Int,
                           day: String,
                           hour: Int,
                           tags: Map[String,String]) extends Measure


