package com.hurence.solr

import com.hurence.historian.model.ChunkRecordV0
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSolrUtils {
  def loadTimeSeriesFromSolR(spark: SparkSession, solrOpts: Map[String, String]): Dataset[ChunkRecordV0] = {
    //TODO
    return null;
//    spark.read
//      .format("solr")
//      .options(solrOpts)
//      .load
//      .map(r => new TimeSeriesRecord("evoa_measure",
//        r.getAs[String]("name"),
//        r.getAs[String]("chunk_value"),
//        r.getAs[Long]("chunk_start"),
//        r.getAs[Long]("chunk_end")))(org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord])
  }

  def loadFromSolR(spark: SparkSession, solrOpts: Map[String, String]): DataFrame = {
    spark.read
      .format("solr")
      .options(solrOpts)
      .load
  }
}
