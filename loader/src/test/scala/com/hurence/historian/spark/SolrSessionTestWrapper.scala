package com.hurence.historian.spark



import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.core.CoreContainer
import org.apache.spark.sql.DataFrame


/**
  * Initialize a solr embedded server for unit tests
  */
trait SolrSessionTestWrapper {


  lazy val container:CoreContainer = {
    val filePath = this.getClass.getClassLoader.getResource("testdata/solr").getPath
    val coreContainer = new CoreContainer(filePath)
    coreContainer.load()
    coreContainer
  }

  lazy val server:EmbeddedSolrServer = {
    new EmbeddedSolrServer(container, "historian")
  }

}
