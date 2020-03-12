package com.hurence.historian

import java.util

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import com.hurence.historian.ReducingChunkSizeChunkCompactorJob2Test.LOGGER
import com.hurence.historian.solr.injector.GeneralSolrInjector
import com.hurence.historian.solr.util.SolrITHelper
import com.hurence.logisland.record.{Point, TimeSeriesRecord}
import com.hurence.unit5.extensions.{SolrExtension, SparkExtension}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.solr.common.params.SolrParams
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeAll, Disabled, Test}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.DockerComposeContainer

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[SolrExtension], classOf[SparkExtension]))
class ReducingChunkSizeChunkCompactorJob2Test(container : (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]}) {

    val compactorConf = ChunkCompactorConf(SolrExtension.getZkUrl(container),  SolrITHelper.COLLECTION_HISTORIAN,
        chunkSize = 2,
        saxAlphabetSize = 2,
        saxStringLength = 3,
        year = ReducingChunkSizeChunkCompactorJob2Test.year,
        month = ReducingChunkSizeChunkCompactorJob2Test.month,
        day = ReducingChunkSizeChunkCompactorJob2Test.day)

    val compactor = new ChunkCompactorJobStrategy2(compactorConf)
    val metricA: String = ReducingChunkSizeChunkCompactorJob2Test.metricA
    val metricB: String = ReducingChunkSizeChunkCompactorJob2Test.metricB


  @Test
  def testCompactor(sparkSession: SparkSession, client: SolrClient) = {
    val start = System.currentTimeMillis();
    assertEquals(2, IncreasingChunkSizeChunkCompactorJob2Test.docsInSolr(client))
    //LOADING
    val loadedFromSolr = compactor.loadDataFromSolR(sparkSession, s"name:*")
    loadedFromSolr.cache()
    loadedFromSolr.show(100)
    assertEquals(2, loadedFromSolr.count())
    //CHUNKING
    val chunked = compactor.mergeChunks(sparkSession, loadedFromSolr)
    chunked.show(100)
    chunked.show(100)
    val records: util.List[TimeSeriesRecord] = chunked.collectAsList()
    //TODO uncomment commented tests. There is currently a chunk of size 0 that should not exist here !
    assertEquals(12, records.size())
    val recordsA: List[TimeSeriesRecord] = records.filter(r => r.getMetricName==metricA).toList
    //        assertEquals(6, recordsA.size())
    val pointsA: List[Point] = recordsA.flatMap(_.getPoints)
    assertEquals(12, pointsA.size())
    //first
    assertEquals(1477895624866L, recordsA.get(0).getStartChunk)
    assertEquals(1477916224866L, recordsA.get(0).getEndChunk)
    //last
    assertEquals(1477925224866L, recordsA.get(5).getStartChunk)
    assertEquals(1477926224866L, recordsA.get(5).getEndChunk)
    val recordsB: List[TimeSeriesRecord] = records.filter(r => r.getMetricName==metricB).toList
    //        assertEquals(6, recordsB.size())
    val pointsB: List[Point] =recordsB.flatMap(_.getPoints)
    assertEquals(12, pointsB.size())
    //first
    assertEquals(1477895624866L, recordsB.get(0).getStartChunk)
    assertEquals(1477916224866L, recordsB.get(0).getEndChunk)
    //last
    assertEquals(1477925224866L, recordsB.get(5).getStartChunk)
    assertEquals(1477926224866L, recordsB.get(5).getEndChunk)
    assertEquals(pointsA, pointsB)

    //If we suppose ancient chunk are not deleted !
    //SAVING
    val savedDf = compactor.saveNewChunksToSolR(chunked)
    savedDf.show(100)
    assertEquals(14, IncreasingChunkSizeChunkCompactorJob2Test.docsInSolr(client))
    val end = System.currentTimeMillis();
    LOGGER.info("compactor finished in {} s", (end - start) / 1000)
  }
}

object ReducingChunkSizeChunkCompactorJob2Test {
  private val LOGGER = LoggerFactory.getLogger(classOf[ReducingChunkSizeChunkCompactorJob2Test])
  private val year = 1999;
  private val month = 10;
  private val day = 1;
  private val chunkOrigin = "logisland";
  private val metricA = "temp_a"
  private val metricB = "temp_b"

  @BeforeAll
  def initHistorianAndDeployVerticle(client: SolrClient): Unit = {
    SolrITHelper.initHistorianSolr(client)
    LOGGER.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
    val injector: GeneralSolrInjector = new GeneralSolrInjector()
    injector.addChunk(metricA, year, month, day, chunkOrigin,
      util.Arrays.asList(
        new Point(0, 1477895624866L, 622),
        new Point(0, 1477916224866L, -3),
        new Point(0, 1477917224866L, 365),
        new Point(0, 1477918224866L, 120),
        new Point(0, 1477919224866L, 15),
        new Point(0, 1477920224866L, -100),
        new Point(0, 1477921224866L, 0),
        new Point(0, 1477922224866L, 120),
        new Point(0, 1477923224866L, 250),
        new Point(0, 1477924224866L, 275),
        new Point(0, 1477925224866L, 288),
        new Point(0, 1477926224866L, 198)
      )//12
    )
    injector.addChunk(metricB, year, month, day, chunkOrigin,
      util.Arrays.asList(
        new Point(0, 1477895624866L, 622),
        new Point(0, 1477916224866L, -3),
        new Point(0, 1477917224866L, 365),
        new Point(0, 1477918224866L, 120),
        new Point(0, 1477919224866L, 15),
        new Point(0, 1477920224866L, -100),
        new Point(0, 1477921224866L, 0),
        new Point(0, 1477922224866L, 120),
        new Point(0, 1477923224866L, 250),
        new Point(0, 1477924224866L, 275),
        new Point(0, 1477925224866L, 288),
        new Point(0, 1477926224866L, 198)
      )
    )
    injector.injectChunks(client)
    LOGGER.info("Indexed some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
  }

  def docsInSolr(client: SolrClient) = {
    val params: SolrParams = new SolrQuery("*:*");
    val rsp: QueryResponse = client.query(SolrITHelper.COLLECTION_HISTORIAN, params)
    rsp.getResults.getNumFound
  }
}



