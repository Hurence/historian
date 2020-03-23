package com.hurence.historian

import org.apache.solr.client.solrj.SolrClient
import org.junit.jupiter.api.Disabled
import org.testcontainers.containers.DockerComposeContainer

@Disabled("legacy, should be removed soon")
class IncreasingChunkSizeStrategy1Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractIncreasingChunkSizeTest(container) {

  val compactorConf: ChunkCompactorConf = ChunkCompactorConf(zkUrl, historianCollection,
    chunkSize = 10,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day)

  override def createCompactor() = {
    new ChunkCompactorJobStrategy1(compactorConf)
  }

  override def testReportEnd(client: SolrClient): Unit = {
    //no report
  }
}
