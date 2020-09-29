package com.hurence.historian.spark.sql.writer.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.writer.Writer
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import com.hurence.historian.spark.sql.functions._
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._

import scala.collection.JavaConverters._

/**
  * val options.config = Map(
  * "zkhost" -> options.zkHosts,
  * "collection" -> options.collectionName
  * )
  *
  */
class SolrChunksWriter extends Writer[Chunk] {


  private val logger = LoggerFactory.getLogger(classOf[SolrChunksWriter])

  override def write(options: Options, ds: Dataset[_ <: Chunk]): Unit = {

    logger.info(s"start saving new chunks to ${options.config("collection")}")

    val config = if (!options.config.contains("flatten_multivalued"))
      options.config + ("flatten_multivalued" -> "false")
    else
      options.config

    val tagCols = options.config("tag_names").split(",").toList
      .map(tag => col("tags")(tag).as(tag))
    val mainCols = FIELDS.asScala.toList
      .map(name => col(name).as(getColumnFromField(name)))

    // todo manage dateFormatbucket and date interval
    ds
      .select(mainCols ::: tagCols: _*)
      .withColumn(SOLR_COLUMN_VALUE, toBase64(col(SOLR_COLUMN_VALUE)))
      .write
      .format("solr")
      .options(config)
      .save()


  }

}
