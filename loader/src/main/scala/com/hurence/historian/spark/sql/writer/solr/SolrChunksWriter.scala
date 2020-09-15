package com.hurence.historian.spark.sql.writer.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.functions._
import com.hurence.historian.spark.sql.writer.Writer
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
  * val options.config = Map(
  * "zkhost" -> options.zkHosts,
  * "collection" -> options.collectionName
  * )
  *
  */
class SolrChunksWriter extends Writer[ChunkVersionCurrent] {


  private val logger = LoggerFactory.getLogger(classOf[SolrChunksWriter])

  override def write(options: Options, ds: Dataset[_ <: ChunkVersionCurrent]): Unit = {

    logger.info(s"start saving new chunks to ${options.config("collection")}")


    val config = if (options.config.get("flatten_multivalued").isEmpty)
      options.config + ("flatten_multivalued" -> "false")
    else
      options.config


    val tagNames = options.config("tag_names")
      .split(",").toList
    val tagCols = tagNames.map(tag => col("tags")(tag).as(tag))
    val mainCols = List( "day", "start", "end", "count", "avg", "stddev", "min", "max", "first", "last", "sax", "value")
      .map(name => col(name).as(s"chunk_$name")) ::: List("name").map(col)


    ds
      .withColumn("value", col("chunk"))
      .select(mainCols ::: tagCols: _*)
      .withColumn("id", sha256(col("chunk_value")))
      .write
      .format("solr")
      .options(config)
      .save()


  }

}
