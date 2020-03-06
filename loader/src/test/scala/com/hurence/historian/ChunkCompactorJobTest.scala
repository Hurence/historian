package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorOptions
import com.hurence.logisland.record.TimeSeriesRecord
import com.hurence.unit5.extensions.SolrExtension
import org.apache.solr.client.solrj.SolrClient
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.{Logger, LoggerFactory}
import org.junit.jupiter.api.Assertions._

@ExtendWith(Array(classOf[SolrExtension]))
class ChunkCompactorJobTest {
    private val LOGGER = LoggerFactory.getLogger(classOf[ChunkCompactorJobTest])

    val testOptions = ChunkCompactorOptions("local[2]", "zookeeper:2181", "historian", "loader-test", 10, 2, 2, false, 2019, 6, 19)
    val compactor = new ChunkCompactorJob(testOptions)

    private var loadedFromSolr: Dataset[TimeSeriesRecord] = null
    private var chunked: Dataset[TimeSeriesRecord] = null


    @Test
    def testOK(client: SolrClient) = {
        LOGGER.info("client : {}", client)
        assertTrue(true)
    }

    @Test
    def testLoading(sparkSession: SparkSession) = {
//        loadedFromSolr = compactor.loadDataFromSolR(sparkSession, s"name:*")
        //TODO test it
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


