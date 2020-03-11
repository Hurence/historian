package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import com.hurence.historian.solr.util.SolrITHelper
import com.hurence.unit5.extensions.SparkExtension
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
//import org.apache.spark.sql.functions

@ExtendWith(Array(classOf[SparkExtension]))
class MyJobTest {
  private val LOGGER = LoggerFactory.getLogger(classOf[MyJobTest])
  private val year = 2020;
  private val month = 3;
  private val day = 1;
  private val chunkOrigin = "logisland";
  private val metricA = "temp_a"
  private val metricB = "temp_b"
  private val zkUrl = "localhost:9983"

  val compactorConf: ChunkCompactorConf = ChunkCompactorConf(zkUrl, SolrITHelper.COLLECTION_HISTORIAN,
    chunkSize = 10,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day)

  val compactor = new ChunkCompactorJobStrategy2(compactorConf)



  @Test
  def testCompactor(sparkSession: SparkSession) = {
//    val clientBuilder = new CloudSolrClient.Builder(
//      Arrays.asList(zkUrl),
//      Optional.empty());
//    val client = clientBuilder.withConnectionTimeout(10000).withSocketTimeout(60000).build

    val start = System.currentTimeMillis();
//    assertEquals(24, IncreasingChunkSizeChunkCompactorJobTest.docsInSolr(client))
    val loadedFromSolr = compactor.loadDataFromSolR(sparkSession, s"name:*")
//    assertEquals(1380374, loadedFromSolr.count())
//    loadedFromSolr.show(10,false)
    val compactedChunks = compactor.mergeChunks(sparkSession = loadedFromSolr.sparkSession, loadedFromSolr)
    compactedChunks.show(10, false)
    LOGGER.info("compactedChunks count {}", compactedChunks.count())
//    loadedFromSolr.unpersist()
//    chunked.cache()
//    val savedDf = testTransformingAndSavingIntoSolr(chunked)
//    savedDf.cache()
//    //If we suppose ancient chunk are not deleted !
//    assertEquals(28, IncreasingChunkSizeChunkCompactorJobTest.docsInSolr(client))
    val end = System.currentTimeMillis();
    LOGGER.info("compactor finished in {} s", (end - start) / 1000)
  }

}





