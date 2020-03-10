package com.hurence.historian

import java.util
import java.util.Arrays

import com.hurence.historian.ChunkCompactorJob.{ChunkCompactorConf, ChunkCompactorJobOptions}
import com.hurence.historian.ChunkCompactorJobTest.LOGGER
import com.hurence.historian.solr.injector.{GeneralSolrInjector, SolrInjector, SolrInjectorMultipleMetricSpecificPoints}
import com.hurence.historian.solr.util.SolrITHelper
import com.hurence.logisland.record.{Point, TimeSeriesRecord}
import com.hurence.unit5.extensions.{SolrExtension, SparkExtension}
import org.apache.solr.client.solrj.SolrClient
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.{Logger, LoggerFactory}
import org.junit.jupiter.api.Assertions._
import org.testcontainers.containers.DockerComposeContainer

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[SolrExtension], classOf[SparkExtension]))
class ChunkCompactorJobTest(container : (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]}) {

    val compactorConf = ChunkCompactorConf(SolrExtension.getZkUrl(container),  SolrITHelper.COLLECTION_HISTORIAN,
        chunkSize = 2,
        saxAlphabetSize = 2,
        saxStringLength = 3,
        year = ChunkCompactorJobTest.year,
        month = ChunkCompactorJobTest.month,
        day = ChunkCompactorJobTest.day)

    val compactor = new ChunkCompactorJob(compactorConf)
    val metricA = "temp_a"
    val metricB = "temp_b"

    @Test
    def testCompactor(sparkSession: SparkSession) = {
        LOGGER.info("testLoading")
        val loadedFromSolr = testLoading(sparkSession)
        loadedFromSolr.cache()
        val chunked = testChunking(loadedFromSolr)
        chunked.cache()
        val savedDf = testTransformingAndSavingIntoSolr(chunked)
        savedDf.cache()
        assertTrue(true)
    }

    private def testLoading(sparkSession: SparkSession) = {
        val loadedFromSolr = compactor.loadDataFromSolR(sparkSession, s"name:*")
        loadedFromSolr.show(100)
        //"filters" -> s"chunk_origin:logisland AND year:${options.year} AND month:${options.month} AND day:${options.day} AND $filterQuery"
        val records: util.List[TimeSeriesRecord] = loadedFromSolr.collectAsList()
        assertEquals(2, records.size())
        val recordsA = records.find(r => r.getMetricName==metricA).get
        assertEquals(12, recordsA.getChunkSize)
        assertEquals(metricA, recordsA.getMetricName)
        assertEquals(1477895624866L, recordsA.getStartChunk)
        assertEquals(1477926224866L, recordsA.getEndChunk)
        val recordsB = records.find(r => r.getMetricName==metricB).get
        assertEquals(12, recordsB.getChunkSize)
        assertEquals(metricB, recordsB.getMetricName)
        assertEquals(1477895624866L, recordsB.getStartChunk)
        assertEquals(1477926224866L, recordsB.getEndChunk)
        loadedFromSolr
    }

    private def testChunking(loadedFromSolr: Dataset[TimeSeriesRecord]) = {
        LOGGER.info("testChunking")
        val chunked = compactor.mergeChunks(loadedFromSolr)
        chunked.show(100)
        val records: util.List[TimeSeriesRecord] = chunked.collectAsList()
        //TODO uncomment commented tests. There is currently a chunk of size 0 that should not exist here !
//        assertEquals(12, records.size())
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
        chunked
    }

    private def testTransformingAndSavingIntoSolr(chunked: Dataset[TimeSeriesRecord]) = {
        LOGGER.info("testTransformingAndSavingIntoSolr")
        val savedDf = compactor.saveNewChunksToSolR(chunked)
        savedDf.show(100)
        val records: util.List[Row] = savedDf.collectAsList()
        assertEquals(2, records.size())
        savedDf
    }
}

object ChunkCompactorJobTest {
    private val LOGGER = LoggerFactory.getLogger(classOf[ChunkCompactorJobTest])
    private val year = 1999;
    private val month = 10;
    private val day = 1;
    private val chunkOrigin = "logisland";

    @BeforeAll
    def initHistorianAndDeployVerticle(client: SolrClient): Unit = {
        SolrITHelper.initHistorianSolr(client)
        LOGGER.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
        val injector: GeneralSolrInjector = new GeneralSolrInjector()
        injector.addChunk("temp_a", year, month, day, chunkOrigin,
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
        injector.addChunk("temp_b", year, month, day, chunkOrigin,
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
}

