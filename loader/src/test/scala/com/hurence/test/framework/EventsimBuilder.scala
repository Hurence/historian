package com.hurence.test.framework

import java.util.UUID

import com.hurence.historian.spark.solr.SolrCloudUtil
import com.hurence.historian.spark.solr.util.EventsimUtil

// Builder to be used by all the tests that need Eventsim data. All test methods will re-use the same collection name
trait EventsimBuilder extends SparkSolrTests {

  val collectionName: String = "EventsimTest-" + UUID.randomUUID().toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, numShards, cloudClient, sc)
    EventsimUtil.defineTextFields(cloudClient, collectionName)
    EventsimUtil.loadEventSimDataSet(zkHost, collectionName, sparkSession)
  }

  override def afterAll(): Unit = {
    SolrCloudUtil.deleteCollection(collectionName, cluster)
    super.afterAll()
  }

  def eventSimCount: Int = 1000

  def fieldsCount: Int = 19

  def numShards: Int = 2
}
