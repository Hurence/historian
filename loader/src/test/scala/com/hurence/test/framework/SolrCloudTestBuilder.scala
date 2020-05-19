package com.hurence.test.framework

import java.io.File

import com.hurence.historian.TestSolrCloudClusterSupport
import com.hurence.solr.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert.assertTrue
import org.restlet.ext.servlet.ServerServlet
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SolrCloudTestBuilder extends BeforeAndAfterAll with LazyLogging { this: Suite =>

  @transient var cluster: MiniSolrCloudCluster = _
  @transient var cloudClient: CloudSolrClient = _
  var zkHost: String = _
  var testWorkingDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    System.setProperty("jetty.testMode", "true")
   // val solrXml = new File("src/test/resources/solr.xml")
    val solrXml = new File(this.getClass.getClassLoader.getResource("solr-embedded-conf/solr.xml").getPath)
    val solrXmlContents: String = TestSolrCloudClusterSupport.readSolrXml(solrXml)

    val targetDir = new File("target")
    if (!targetDir.isDirectory)
      fail("Project 'target' directory not found at :" + targetDir.getAbsolutePath)

    testWorkingDir = new File(targetDir, "scala-solrcloud-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    // need the schema stuff
    val extraServlets: java.util.SortedMap[ServletHolder, String] = new java.util.TreeMap[ServletHolder, String]()

    val solrSchemaRestApi : ServletHolder = new ServletHolder("SolrSchemaRestApi", classOf[ServerServlet])
    solrSchemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi")
    extraServlets.put(solrSchemaRestApi, "/schema/*")

    cluster = new MiniSolrCloudCluster(1, null /* hostContext */,
      testWorkingDir.toPath, solrXmlContents, extraServlets, null /* extra filters */)
    cloudClient = cluster.getSolrClient
    cloudClient.connect()

    assertTrue(!cloudClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
    zkHost = cluster.getZkServer.getZkAddress
  }

  override def afterAll(): Unit = {
    cloudClient.close()
    cluster.shutdown()

    if (testWorkingDir != null && testWorkingDir.isDirectory) {
      FileUtils.deleteDirectory(testWorkingDir)
    }

    super.afterAll()
  }

}
