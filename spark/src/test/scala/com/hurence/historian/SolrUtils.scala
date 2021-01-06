package com.hurence.historian

import io.vertx.core.json.JsonArray
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.solr.common.params.SolrParams

object SolrUtils {

  def numberOfDocsInCollection(client: SolrClient, collection: String) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(collection, params)
    rsp.getResults.getNumFound
  }

  def getDocsAsJsonObjectInCollection(client: SolrClient, collection: String) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(collection, params)
    val solrDocuments = rsp.getResults
    val docs: JsonArray = new JsonArray(new java.util.ArrayList(solrDocuments))
    docs
  }
}
