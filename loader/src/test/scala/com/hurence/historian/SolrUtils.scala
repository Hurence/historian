package com.hurence.historian

import com.hurence.historian.solr.util.SolrITHelper
import com.hurence.logisland.record.TimeSeriesRecord
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.SolrParams
import org.apache.spark.sql.{Dataset, SparkSession}

object SolrUtils {
  def docsInSolr(client: SolrClient) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(SolrITHelper.COLLECTION_HISTORIAN, params)
    rsp.getResults.getNumFound
  }

  def loadTimeSeriesFromSolR(spark: SparkSession, solrOpts: Map[String, String]): Dataset[TimeSeriesRecord] = {
    spark.read
      .format("solr")
      .options(solrOpts)
      .load
      .map(r => new TimeSeriesRecord("evoa_measure",
        r.getAs[String]("name"),
        r.getAs[String]("chunk_value"),
        r.getAs[Long]("chunk_start"),
        r.getAs[Long]("chunk_end")))(org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord])
  }
}
