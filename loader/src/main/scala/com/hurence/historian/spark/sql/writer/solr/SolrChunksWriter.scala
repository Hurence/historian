package com.hurence.historian.spark.sql.writer.solr

import com.hurence.historian.model.ChunkRecordV0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.writer.Writer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory


/**
  * val options.config = Map(
  *    "zkhost" -> options.zkHosts,
  *    "collection" -> options.collectionName
  * )
  *
  */
class SolrChunksWriter extends Writer[ChunkRecordV0] {


  private val logger = LoggerFactory.getLogger(classOf[SolrChunksWriter])

  override def write(options: Options, ds: Dataset[ChunkRecordV0]): Unit = {

    logger.info(s"start saving new chunks to ${options.config("collection")}")

    ds
      .drop("timestamps", "values")
      .write
      .format("solr")
      .options(options.config)
      .save()
  }

}
