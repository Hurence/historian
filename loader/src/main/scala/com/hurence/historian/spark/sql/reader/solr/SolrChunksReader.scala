package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.spark.common.Definitions._
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Chunk
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

class SolrChunksReader extends Reader[Chunk] {

  override def read(options: Options): Dataset[_ <: Chunk] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get


    val tagNames: List[Column] = options.config(Options.TAG_NAMES)
      .split(",").toList
      .map(tag => col(tag))
    val mainCols = List("day", "start", "end", "count", "avg", "std_dev", "min", "max", "first", "last", "sax", "value",
      "origin", "quality_min", "quality_max", "quality_first", "quality_sum", "quality_avg")
      .map(name => col(s"chunk_$name").as(name)) ::: List("name").map(col) ::: List("id").map(col) ::: tagNames


    val tags: List[Column] = options.config("tag_names")
      .split(",").toList
      .flatMap(tag => List(lit(tag), col(tag)))

    spark.read
      .format("solr")
      .options(options.config)
      .load()
      .select(mainCols: _*)
      .withColumn(CHUNK_COLUMN, col("value"))
      .withColumn("tags", map(tags: _*))
      .as[Chunk](Encoders.bean(classOf[Chunk]))
  }
}
