package com.hurence.historian

import net.liftweb.json.Serialization.write
import net.liftweb.json._

case class ChunkCompactorConfStrategy2(zkHosts: String,
                                       timeseriesCollectionName: String,
                                       reportCollectionName: String,
                                       chunkSize: Int,
                                       saxAlphabetSize: Int,
                                       saxStringLength: Int,
                                       year: Int,
                                       month: Int,
                                       day: Int,
                                       solrFq: String) {

  def toJsonStr: String = {
    implicit val formats = DefaultFormats
    val jsonString = write(this)
    jsonString
  }
}
