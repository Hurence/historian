package com.hurence.historian

import com.hurence.historian.solr.util.SolrITHelper
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.SolrParams

object SolrUtils {
  def docsInSolr(client: SolrClient) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(SolrITHelper.COLLECTION_HISTORIAN, params)
    rsp.getResults.getNumFound
  }
}
