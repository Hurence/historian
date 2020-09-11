package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.modele.chunk.{ChunkVersion0, ChunkVersion0Impl}
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

class SolrChunksReader extends Reader[ChunkVersion0] {

  override def read(options: Options): Dataset[_ <: ChunkVersion0] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get


    val tagNames: List[Column] = options.config(Options.TAG_NAMES)
      .split(",").toList
      .map(tag => col(tag))
    val mainCols = List("day", "start", "end", "count", "avg", "stddev", "min", "max", "first", "last", "sax", "value")
      .map(name => col(s"chunk_$name").as(name)) ::: List("name").map(col) ::: tagNames


    val tags: List[Column] = options.config("tag_names")
      .split(",").toList
      .flatMap(tag => List(lit(tag), col(tag)))

    spark.read
      .format("solr")
      .options(options.config)
      .load()
      .select(mainCols: _*)
      .withColumn("chunk", col("value"))
      .withColumn("tags", map(tags: _*))
      .as[ChunkVersion0Impl](Encoders.bean(classOf[ChunkVersion0Impl]))

  }

}
