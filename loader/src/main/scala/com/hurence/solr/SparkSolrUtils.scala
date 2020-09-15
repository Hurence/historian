package com.hurence.solr

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSolrUtils {
//  def loadTimeSeriesFromSolR(spark: SparkSession, solrOpts: Map[String, String]): Dataset[ChunkVersion0] = {
//    return null;
//  }

  def loadFromSolR(spark: SparkSession, solrOpts: Map[String, String]): DataFrame = {
    spark.read
      .format("solr")
      .options(solrOpts)
      .load
  }
}
