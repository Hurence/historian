package com.hurence.historian

import java.util
import java.util.Arrays

import com.hurence.historian.ChunkCompactorJob.{ChunkCompactorConf, ChunkCompactorJobOptions}
import com.hurence.historian.solr.injector.{SolrInjector, SolrInjectorMultipleMetricSpecificPoints}
import com.hurence.historian.solr.util.SolrITHelper
import com.hurence.logisland.record.{Point, TimeSeriesRecord}
import com.hurence.unit5.extensions.{SolrExtension, SparkExtension}
import org.apache.solr.client.solrj.SolrClient
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.{Logger, LoggerFactory}
import org.junit.jupiter.api.Assertions._
import org.testcontainers.containers.DockerComposeContainer

@ExtendWith(Array(classOf[SolrExtension], classOf[SparkExtension]))
class ChunkCompactorJobTest(container : (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]}) {

    //    val testOptions = ChunkCompactorJobOptions("local[]", "loader-test", false, SolrExtension.getZkUrl(container),  SolrITHelper.COLLECTION_HISTORIAN, 1440, 7, 100, 2019, 6, 19)
    val compactorConf = ChunkCompactorConf(SolrExtension.getZkUrl(container),  SolrITHelper.COLLECTION_HISTORIAN, 1440, 7, 100, 2019, 6, 19)
    val compactor = new ChunkCompactorJob(compactorConf)

    private var loadedFromSolr: Dataset[TimeSeriesRecord] = null
    private var chunked: Dataset[TimeSeriesRecord] = null

    @Test
    def testLoading(sparkSession: SparkSession) = {
        loadedFromSolr = compactor.loadDataFromSolR(sparkSession, s"name:*")
        loadedFromSolr.show(100)
        assertTrue(true)
    }

    @Test
    def testChunking() = {
//        chunked = compactor.mergeChunks(loadedFromSolr)
        //TODO test it
        assertTrue(true)
    }

    @Test
    def testTransformingAndSavingIntoSolr() = {
//        val savedDf = compactor.saveNewChunksToSolR(chunked)
        //TODO test it
        assertTrue(true)
    }
}

object ChunkCompactorJobTest {
    private val LOGGER = LoggerFactory.getLogger(classOf[ChunkCompactorJobTest])

    @BeforeAll
    def initHistorianAndDeployVerticle(client: SolrClient): Unit = {
        SolrITHelper.initHistorianSolr(client)
        LOGGER.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
        val injector: SolrInjector =
            new SolrInjectorMultipleMetricSpecificPoints(
                util.Arrays.asList("temp_a", "temp_b", "maxDataPoints"),
                util.Arrays.asList(
                    util.Arrays.asList(
                        new Point(0, 1477895624866L, 622),
                        new Point(0, 1477916224866L, -3),
                        new Point(0, 1477917224866L, 365)),
                    util.Arrays.asList(new Point(0, 1477895624866L, 861),
                        new Point(0, 1477917224866L, 767)),
                    util.Arrays.asList( //maxDataPoints we are not testing value only sampling
                        new Point(0, 1477895624866L, 1),
                        new Point(0, 1477895624867L, 1),
                        new Point(0, 1477895624868L, 1),
                        new Point(0, 1477895624869L, 1),
                        new Point(0, 1477895624870L, 1),
                        new Point(0, 1477895624871L, 1),
                        new Point(0, 1477895624872L, 1),
                        new Point(0, 1477895624873L, 1),
                        new Point(0, 1477895624874L, 1),
                        new Point(0, 1477895624875L, 1),
                        new Point(0, 1477895624876L, 1))
                ))
        injector.injectChunks(client)
        LOGGER.info("Indexed some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
    }
}

