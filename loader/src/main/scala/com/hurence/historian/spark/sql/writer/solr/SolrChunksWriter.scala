package com.hurence.historian.spark.sql.writer.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.Options.{FLATTEN_MULTIVALUED, TAG_NAMES}
import com.hurence.historian.spark.sql.writer.Writer
import org.apache.spark.sql.{Column, Dataset}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
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

    val config = if (!options.config.contains(FLATTEN_MULTIVALUED))
      options.config + (FLATTEN_MULTIVALUED -> "false")
    else
      options.config

    var someTags : Boolean = true
    val tagCols : List[Column] = if (options.config.contains(TAG_NAMES)) {
      options.config(TAG_NAMES).split(",").toList
        .map(tag => col(FIELD_TAGS)(tag).as(tag))
    } else  {
      // No tags specified
      someTags = false
      List[Column]()
    }

    val mainCols = FIELDS.asScala.toList
      .map(name => col(name).as(getColumnFromField(name)))

    // todo manage dateFormatbucket and date interval
    ds
      .select(mainCols ::: tagCols: _*)
      .withColumn(SOLR_COLUMN_VALUE, base64(col(SOLR_COLUMN_VALUE)))
      .write
      .format("solr")
      .options(config)
      .save()
  }
}
