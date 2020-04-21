package com.hurence.historian.model


/**
  * A chunk is a group of consecutive measures
  *
  * @see Measure
  */
sealed trait Chunk {

  def apiVersion: String = "v0"

  def name: String

  def start: Long

  def end: Long

  def value: String

  def count: Long

  def first: Double

  def last: Double

  def min: Double

  def max: Double

  def avg: Double

  def sax: String

}


// kurtosis
//
/**
  * Chunk Record v0
  *
  * @param name
  * @param day
  * @param start
  * @param end
  * @param value
  * @param timestamps
  * @param values
  * @param count
  * @param avg
  * @param stddev
  * @param min
  * @param max
  * @param first
  * @param last
  * @param sax
  * @param tags
  */
case class ChunkRecordV0(name: String,
                         day:String,
                         start: Long,
                         end: Long,
                         value: String,
                         timestamps: Array[Long],
                         values: Array[Double],
                         count: Long,
                         avg: Double,
                         stddev: Double,
                         min: Double,
                         max: Double,
                         first: Double,
                         last: Double,
                         sax: String,
                         tags: Map[String,String]) extends Chunk

