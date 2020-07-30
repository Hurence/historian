package com.hurence.historian.modele


trait HistorianRecord {
  def name: String
}


/**
  * A chunk is a group of consecutive measures
  *
  * @see Measure
  */
trait Chunk extends HistorianRecord {

  def apiVersion: String = SchemaVersion.VERSION_0.toString

  def name: String

  def start: Long

  def end: Long

  def count: Long

  def first: Double

  def last: Double

  def min: Double

  def max: Double

//  def sum: Double

  def avg: Double

  def stddev: Double

  def sax: String

//  def origin: String
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
//                         month: Int,
//                         year: Int,
//                         hour: Int,
                         start: Long,
                         end: Long,
                         chunk: String,
                         count: Long,
                         avg: Double,
                         stddev: Double,
                         min: Double,
                         max: Double,
//                         sum: Double,
                         first: Double,
                         last: Double,
                         sax: String,
//                         origin: String,
//                         compactions_running : List[String],
                         tags: Map[String,String]) extends Chunk

