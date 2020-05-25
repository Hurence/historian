package com.hurence.test.framework

import java.util.UUID

import com.hurence.historian.SolrCloudUtilForTests
import com.hurence.historian.spark.solr.util.EventsimUtil

// Builder to be used by all the tests that need Eventsim data. All test methods will re-use the same collection name
trait EventsimBuilder extends SparkSolrTests {

  val collectionName: String = "EventsimTest-" + UUID.randomUUID().toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    SolrCloudUtilForTests.buildChunkCollection(collectionName, null, numShards, cloudClient)
    EventsimUtil.defineTextFields(cloudClient, collectionName)
    EventsimUtil.loadEventSimDataSet(zkAddressSolr, collectionName, sparkSession)
  }

  override def afterAll(): Unit = {
    SolrCloudUtilForTests.deleteCollection(collectionName, cloudClient)
    super.afterAll()
  }

  def eventSimCount: Int = 1000

  def fieldsCount: Int = 19

  def numShards: Int = 2
}
