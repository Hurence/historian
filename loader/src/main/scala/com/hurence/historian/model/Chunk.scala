package com.hurence.historian.model


sealed trait Chunk {

  def apiVersion: String = "v0"

  def name: String

  def lat: Option[Double]

  def lon: Option[Double]

  def start: Long

  def end: Long

  def value: String

  def quality: String

  def count: Int

  def sizeInBytes: Int

  def first: Double

  def last: Double

  def min: Double

  def max: Double

  def avg: Double

  def sax: String

  def trend: Boolean

  def outlier: Boolean

  def origin: Option[String]

}

case class ChunkRecordV0(name: String,
                         start: Long,
                         end: Long,
                         value: String,
                         quality: String,
                         count: Int,
                         sizeInBytes: Int,
                         first: Double,
                         last: Double,
                         min: Double,
                         max: Double,
                         avg: Double,
                         sax: String,
                         trend: Boolean,
                         outlier: Boolean,
                         origin: Option[String],
                         lat: Option[Double],
                         lon: Option[Double]) extends Chunk
