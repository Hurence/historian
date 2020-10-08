package com.hurence.historian.spark.sql

case class Options(path:String, config:Map[String,String])

object Options {
  val TAG_NAMES = "tag_names"
  val FLATTEN_MULTIVALUED = "flatten_multivalued"
}
