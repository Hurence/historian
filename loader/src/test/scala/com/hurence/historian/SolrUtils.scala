package com.hurence.historian

import java.util

import com.hurence.historian.spark.compactor.job.CompactorJobReport
import com.hurence.unit5.extensions.SolrExtension.SOLR_CONF_TEMPLATE_REPORT
import io.vertx.core.json.JsonArray
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.solr.common.params.SolrParams

object SolrUtils {
  def createReportCollection(client: SolrClient) = {
    val createrequest = CollectionAdminRequest.createCollection(CompactorJobReport.DEFAULT_COLLECTION, SOLR_CONF_TEMPLATE_REPORT, 1, 1)
    client.request(createrequest)
  }

  def numberOfDocsInCollection(client: SolrClient, collection: String) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(collection, params)
    rsp.getResults.getNumFound
  }

  def getDocsAsJsonObjectInCollection(client: SolrClient, collection: String) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(collection, params)
    val solrDocuments = rsp.getResults
    val docs: JsonArray = new JsonArray(new util.ArrayList(solrDocuments))
    docs
  }
}
