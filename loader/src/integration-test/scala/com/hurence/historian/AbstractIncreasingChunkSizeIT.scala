//package com.hurence.historian
//
//import java.util
//
//import com.hurence.historian.modele.{HistorianChunkCollectionFieldsVersionEVOA0, SchemaVersion}
//import com.hurence.historian.solr.injector.GeneralVersion0SolrInjector
//import com.hurence.historian.solr.util.SolrITHelper
//import com.hurence.historian.spark.compactor.ChunkCompactor
//import com.hurence.historian.spark.compactor.job.{CompactorJobReport, JobStatus}
//import com.hurence.logisland.record.TimeSeriesRecord
//import com.hurence.solr.SparkSolrUtils
//import com.hurence.timeseries.model.measures.Measure
//import com.hurence.unit5.extensions.{SolrExtension, SparkExtension}
//import io.vertx.core.json.JsonObject
//import org.apache.solr.client.solrj.SolrClient
//import org.apache.spark.sql.SparkSession
//import org.junit.jupiter.api.Assertions._
//import org.junit.jupiter.api.extension.ExtendWith
//import org.junit.jupiter.api.{BeforeAll, Test}
//import org.slf4j.LoggerFactory
//import org.testcontainers.containers.DockerComposeContainer
//
//import scala.collection.JavaConversions._
//
//@ExtendWith(Array(classOf[SolrExtension], classOf[SparkExtension]))
//abstract class AbstractIncreasingChunkSizeIT(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]}) {
//
//  val zkUrl: String = SolrExtension.getZkUrl(container)
//  val historianCollection: String = SolrITHelper.COLLECTION_HISTORIAN
//
//  val chunkSize = 10
//  val year: Int = AbstractIncreasingChunkSizeIT.year
//  val month: Int = AbstractIncreasingChunkSizeIT.month
//  val day: Int = AbstractIncreasingChunkSizeIT.day
//
//  val metricA: String = AbstractIncreasingChunkSizeIT.metricA
//  val metricB: String = AbstractIncreasingChunkSizeIT.metricB
//
//  def createCompactor: ChunkCompactor
//
//  @Test
//  def testCompactor(sparkSession: SparkSession, client: SolrClient) = {
//    //sometime some documents seems to not have been commited ? Will see if sleeping solve this problem
//    Thread.sleep(1000)
//    assertEquals(24, SolrUtils.numberOfDocsInCollection(client, SolrITHelper.COLLECTION_HISTORIAN))
//    createCompactor.run(sparkSession)
//    assertEquals(4, SolrUtils.numberOfDocsInCollection(client, SolrITHelper.COLLECTION_HISTORIAN))
//    testChunks(sparkSession)
//    testReportEnd(client)
//  }
//
//  def testChunks(sparkSession: SparkSession) = {
//    //Test on chunks created
//    val solrOpts = Map(
//      "zkhost" -> zkUrl,
//      "collection" -> historianCollection,
//      "sort" -> "chunk_start asc",
//      "fields" -> "name,chunk_value,chunk_start,chunk_end,chunk_size,year,month,day"
//    )
//    val comapactedChunks = SparkSolrUtils.loadTimeSeriesFromSolR(sparkSession, solrOpts)
//    val records: util.List[TimeSeriesRecord] = comapactedChunks.collectAsList()
//    assertEquals(4, records.size())
//    val recordsA: List[TimeSeriesRecord] = records
//      .filter(r => r.getMetricName == metricA)
//      .sortBy(r => r.getStartChunk)
//      .toList
//    assertEquals(2, recordsA.size())
//    //first
//    assertEquals(1477895624866L, recordsA.get(0).getStartChunk)
//    assertEquals(1477924224866L, recordsA.get(0).getEndChunk)
//    assertEquals(10, recordsA.get(0).getChunkSize)
//    //last
//    assertEquals(1477925224866L, recordsA.get(1).getStartChunk)
//    assertEquals(1477926224866L, recordsA.get(1).getEndChunk)
//    assertEquals(2, recordsA.get(1).getChunkSize)
//    val recordsB: List[TimeSeriesRecord] = records
//      .filter(r => r.getMetricName == metricB)
//      .sortBy(r => r.getStartChunk)
//      .toList
//    assertEquals(2, recordsB.size())
//    //first
//    assertEquals(1477895624866L, recordsB.get(0).getStartChunk)
//    assertEquals(1477924224866L, recordsB.get(0).getEndChunk)
//    assertEquals(10, recordsB.get(0).getChunkSize)
//    //last
//    assertEquals(1477925224866L, recordsB.get(1).getStartChunk)
//    assertEquals(1477926224866L, recordsB.get(1).getEndChunk)
//    assertEquals(2, recordsB.get(1).getChunkSize)
//
//    //Test on measures of chunks
//    val pointsA: List[Measure] = recordsA.flatMap(_.getPoints)
//    assertEquals(12, pointsA.size())
//    val pointsB: List[Measure] = recordsB.flatMap(_.getPoints)
//    assertEquals(12, pointsB.size())
//    assertEquals(pointsA, pointsB)
//    assertEquals(622, pointsA.head.getValue)
//    assertEquals(1477895624866L, pointsA.head.getTimestamp)
//    assertEquals(198, pointsA.last.getValue)
//    assertEquals(1477926224866L, pointsA.last.getTimestamp)
//  }
//
//  def testReportEnd(client: SolrClient): Unit = {
//    val reports = SolrUtils.getDocsAsJsonObjectInCollection(client, CompactorJobReport.DEFAULT_COLLECTION)
//    assertEquals(1, reports.size())
//    val report = reports.getJsonObject(0)
//    assertEquals(JobStatus.SUCCEEDED.toString, report.getString(CompactorJobReport.JOB_STATUS))
//    assertEquals(4, report.getLong(CompactorJobReport.JOB_NUMBER_OF_CHUNK_OUTPUT))
//    assertEquals(CompactorJobReport.JOB_TYPE_VALUE, report.getString(CompactorJobReport.JOB_TYPE))
//    assertEquals(null, report.getString(CompactorJobReport.JOB_ERROR))
//    assertEquals(24, report.getLong(CompactorJobReport.JOB_NUMBER_OF_CHUNK_INPUT))
//    assertEquals(2, report.getLong(CompactorJobReport.JOB_TOTAL_METRICS_RECHUNKED))
//    additionalTestsOnReportEnd(report)
//  }
//
//  def additionalTestsOnReportEnd(report: JsonObject): Unit = {}
//}
//
//object AbstractIncreasingChunkSizeIT {
//  private val LOGGER = LoggerFactory.getLogger(classOf[AbstractIncreasingChunkSizeIT])
//  private val year = 1999;
//  private val month = 10;
//  private val day = 1;
//  private val chunkOrigin = "logisland";
//  private val metricA = "temp_a"
//  private val metricB = "temp_b"
//
//  @BeforeAll
//  def initHistorianAndDeployVerticle(client: SolrClient,
//                                     container: (DockerComposeContainer[SELF]) forSome
//                                     {type SELF <: DockerComposeContainer[SELF]}): Unit = {
//    SolrITHelper.creatingAllCollections(client, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0.toString)
//    SolrITHelper.addFieldToChunkSchema(SolrExtension.getSolr1Url(container), HistorianChunkCollectionFieldsVersionEVOA0.CODE_INSTALL)
//    SolrITHelper.addFieldToChunkSchema(SolrExtension.getSolr1Url(container), HistorianChunkCollectionFieldsVersionEVOA0.SENSOR)
//    LOGGER.info("Indexing some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
//    val injector: GeneralVersion0SolrInjector = new GeneralVersion0SolrInjector()
//    addSeveralChunksForMetric(injector, metricA)
//    addSeveralChunksForMetric(injector, metricB)
//    injector.injectChunks(client)
//    LOGGER.info("Indexed some documents in {} collection", SolrITHelper.COLLECTION_HISTORIAN)
//  }
//
//   def addSeveralChunksForMetric(injector: GeneralVersion0SolrInjector, metric: String) = {
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477895624866L, 622)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477916224866L, -3)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477917224866L, 365)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477918224866L, 120)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477919224866L, 15)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477920224866L, -100)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477921224866L, 0)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure(1477922224866L, 120)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477923224866L, 250)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477924224866L, 275)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477925224866L, 288)
//      )
//    )
//    injector.addChunk(metric, year, month, day, chunkOrigin,
//      util.Arrays.asList(
//        new Measure( 1477926224866L, 198)
//      )
//    )
//  }
//}
//
//
//
//
//
