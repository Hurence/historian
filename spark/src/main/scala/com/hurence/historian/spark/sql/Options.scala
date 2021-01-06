package com.hurence.historian.spark.sql

import org.apache.spark.sql.types.StructType

case class Options(path:String, config:Map[String,String])

object Options {
  val TAG_NAMES = "tag_names"
  val FLATTEN_MULTIVALUED = "flatten_multivalued"
}
