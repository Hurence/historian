package com.hurence.historian.solr

import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.common.cloud.{ClusterState, DocCollection}
import org.noggit.{CharArr, JSONWriter}

import scala.collection.JavaConversions.asScalaSet

object SolrCloudUtil extends LazyLogging {

  def deleteCollection(collectionName: String, client: SolrClient): Unit = {
    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(client)
    } catch {
      case e: Exception => logger.error("Failed to delete collection " + collectionName + " due to: " + e)
    }
  }

  def getClusterStateInfo(collectionName: String, cloudClient: CloudSolrClient): String = {
    cloudClient.getZkStateReader.updateLiveNodes()
    var cs: String = null
    val clusterState: ClusterState = cloudClient.getZkStateReader.getClusterState
    if (collectionName != null) {
      cs = clusterState.getCollection(collectionName).toString
    } else {
      val map = Map.empty[String, DocCollection]
      clusterState.getCollectionsMap.keySet().foreach(coll => {
        map + (coll -> clusterState.getCollection(coll))
      })
      val out: CharArr = new CharArr()
      new JSONWriter(out, 2).write(map)
      cs = out.toString
    }
    cs
  }
}
