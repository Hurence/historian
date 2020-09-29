package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._
import org.apache.spark.sql.functions.{col, lit, map, unbase64}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class SolrChunksReader extends Reader[Chunk] {

  override def read(options: Options): Dataset[_ <: Chunk] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get


    val tagNames: List[Column] = options.config(Options.TAG_NAMES)
      .split(",").toList
      .map(tag => col(tag))
    val mainCols = SOLR_COLUMNS.asScala.toList
      .map(name => col(name).as(getFieldFromColumn(name))) ::: tagNames


    val tags: List[Column] = options.config("tag_names")
      .split(",").toList
      .flatMap(tag => List(lit(tag), col(tag)))

    spark.read
      .format("solr")
      .options(options.config)
      .load()
      .select(mainCols: _*)
      .withColumn(FIELD_VALUE, unbase64(col(FIELD_VALUE)))
      .withColumn(FIELD_TAGS, map(tags: _*))
      .as[Chunk](Encoders.bean(classOf[Chunk]))
  }
}
