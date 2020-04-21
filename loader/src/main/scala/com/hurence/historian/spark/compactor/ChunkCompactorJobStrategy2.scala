package com.hurence.historian.spark.compactor

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.hurence.historian.TimeseriesConverter
import com.hurence.historian.spark.compactor.job.{CompactorJobReport, JobStatus}
import com.hurence.historian.processor.HistorianContext
import com.hurence.historian.solr.HurenceSolrSupport
import com.hurence.logisland.record.{EvoaUtils, TimeseriesRecord}
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.solr.SparkSolrUtils
import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => f}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{WrappedArray => ArrayDF}

class ChunkCompactorJobStrategy2(options: ChunkCompactorConfStrategy2) extends ChunkCompactor {

  private val logger = LoggerFactory.getLogger(classOf[ChunkCompactorJobStrategy2])
  private val totalNumberOfPointColumnName = "number_points_day"
  private val startsColumnName = "starts"
  private val valuesColumnName = "values"
  private val endsColumnName = "ends"
  private val sizesColumnName = "sizes"
  private val nameColumnName = "name"

  private implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeseriesRecord]

  private val jobStart: Long = System.currentTimeMillis()
  private val jobId: String = buildJobId(jobStart)
  private val commitWithinMs = -1



  private def loadChunksFromSolR(spark: SparkSession): DataFrame = {
    val fields = s"${TimeseriesRecord.CHUNK_ID},${TimeseriesRecord.METRIC_NAME},${TimeseriesRecord.CHUNK_VALUE},${TimeseriesRecord.CHUNK_START}," +
      s"${TimeseriesRecord.CHUNK_END},${TimeseriesRecord.CHUNK_SIZE},${TimeseriesRecord.CHUNK_YEAR}," +
      s"${TimeseriesRecord.CHUNK_MONTH},${TimeseriesRecord.CHUNK_DAY}"

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.timeseriesCollectionName,
      "fields" -> fields,
      "filters" -> options.solrFq
    )
    logger.info("solrOpts : {}", solrOpts.mkString("\n{", "\n", "}"))
    SparkSolrUtils.loadFromSolR(spark, solrOpts)
  }

  private def loadChunksToBeTaggegFromSolR(spark: SparkSession): DataFrame = {
    val fields = s"${TimeseriesRecord.CHUNK_ID}"

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.timeseriesCollectionName,
      "fields" -> fields,
      "filters" -> options.solrFq
    )
    logger.info("solrOpts : {}", solrOpts.mkString("\n{", "\n", "}"))
    SparkSolrUtils.loadFromSolR(spark, solrOpts)
  }

  private def loadTaggedChunksFromSolR(spark: SparkSession): DataFrame = {
    val fields = s"${TimeseriesRecord.CHUNK_ID},${TimeseriesRecord.METRIC_NAME},${TimeseriesRecord.CHUNK_VALUE},${TimeseriesRecord.CHUNK_START}," +
      s"${TimeseriesRecord.CHUNK_END},${TimeseriesRecord.CHUNK_SIZE},${TimeseriesRecord.CHUNK_YEAR}," +
      s"${TimeseriesRecord.CHUNK_MONTH},${TimeseriesRecord.CHUNK_DAY}"

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.timeseriesCollectionName,
      /*   "splits" -> "true",
         "split_field"-> "name",
         "splits_per_shard"-> "50",*/
      "fields" -> fields,
      "filters" -> s"""${TimeseriesRecord.CHUNK_COMPACTION_RUNNING}:"$jobId""""
    )
    logger.info("solrOpts : {}", solrOpts.mkString("\n{", "\n", "}"))
    SparkSolrUtils.loadFromSolR(spark, solrOpts)
  }

  private def saveNewChunksToSolR(timeseriesDS: Dataset[TimeseriesRecord]) = {
    import timeseriesDS.sparkSession.implicits._

    logger.info(s"start saving new chunks to ${options.timeseriesCollectionName}")
    val savedDF = timeseriesDS
      .map(r => (
        r.getId,
        r.getField(TimeseriesRecord.CHUNK_YEAR).asInteger(),
        r.getField(TimeseriesRecord.CHUNK_MONTH).asInteger(),
        r.getField(TimeseriesRecord.CHUNK_DAY).asInteger(),
        r.getField(TimeseriesRecord.CODE_INSTALL).asString(),
        r.getField(TimeseriesRecord.SENSOR).asString(),
        r.getField(TimeseriesRecord.METRIC_NAME).asString(),
        r.getField(TimeseriesRecord.CHUNK_VALUE).asString(),
        r.getField(TimeseriesRecord.CHUNK_START).asLong(),
        r.getField(TimeseriesRecord.CHUNK_END).asLong(),
        r.getField(TimeseriesRecord.CHUNK_WINDOW_MS).asLong(),
        r.getField(TimeseriesRecord.CHUNK_SIZE).asInteger(),
        r.getField(TimeseriesRecord.CHUNK_FIRST_VALUE).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_AVG).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_MIN).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_MAX).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_SUM).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_TREND).asBoolean(),
        r.getField(TimeseriesRecord.CHUNK_OUTLIER).asBoolean(),
        if (r.hasField(TimeseriesRecord.CHUNK_SAX)) r.getField(TimeseriesRecord.CHUNK_SAX).asString() else "",
        r.getField(TimeseriesRecord.CHUNK_ORIGIN).asString())
      )
      .toDF(TimeseriesRecord.CHUNK_ID,
        TimeseriesRecord.CHUNK_YEAR,
        TimeseriesRecord.CHUNK_MONTH,
        TimeseriesRecord.CHUNK_DAY,
        TimeseriesRecord.CODE_INSTALL,
        TimeseriesRecord.SENSOR,
        TimeseriesRecord.METRIC_NAME,
        TimeseriesRecord.CHUNK_VALUE,
        TimeseriesRecord.CHUNK_START,
        TimeseriesRecord.CHUNK_END,
        TimeseriesRecord.CHUNK_WINDOW_MS,
        TimeseriesRecord.CHUNK_SIZE,
        TimeseriesRecord.CHUNK_FIRST_VALUE,
        TimeseriesRecord.CHUNK_AVG,
        TimeseriesRecord.CHUNK_MIN,
        TimeseriesRecord.CHUNK_MAX,
        TimeseriesRecord.CHUNK_SUM,
        TimeseriesRecord.CHUNK_TREND,
        TimeseriesRecord.CHUNK_OUTLIER,
        TimeseriesRecord.CHUNK_SAX,
        TimeseriesRecord.CHUNK_ORIGIN)

    if (options.useCache) {
      savedDF.cache()
    }
    val start = System.currentTimeMillis()
    savedDF.write
      .format("solr")
      .options(Map(
        "zkhost" -> options.zkHosts,
        "collection" -> options.timeseriesCollectionName
      ))
      .save()
    logger.info(s"Saving newly compacted chunks took ${(start - System.currentTimeMillis()) / 1000} seconds")
    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.timeseriesCollectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString}")

    savedDF
  }

  private def chunkIntoSeveralTimeseriesRecord(metricType: String, metricName: String,
                                   values: ArrayDF[String],
                                   starts: ArrayDF[Long],
                                   ends: ArrayDF[Long],
                                   sizes: ArrayDF[Long]) = {
    if (
      values.length != starts.length ||
        values.length != ends.length ||
        values.length != sizes.length
    ) {
      throw new RuntimeException("number of chunk_value, chunk_start, chunk_end and chunk_size are not the same ! This should never happen")
    }
    val numberOfChunkToCompact = values.length
    val numberOfChunkToCompactMinusOne = numberOfChunkToCompact - 1

    val chunks = (for (i <- 0 to numberOfChunkToCompactMinusOne) yield {
      CompactionChunkInfo(values(i), starts(i), ends(i), sizes(i))
    }).flatMap(_.getPoints())
      .sortBy(c => c.getTimestamp)
      .grouped(options.chunkSize)
      .map(chunkPoints => {
        val builder = new MetricTimeSeries.Builder(metricName, metricType)
        chunkPoints.foreach(p => builder.point(p.getTimestamp, p.getValue))
        new TimeseriesRecord(builder.build())
      })

    if (logger.isTraceEnabled) {
      chunks.foreach(t => {
        logger.trace(s"${metricName} : new chunk size ${t.getChunkSize}, start ${t.getTimeSeries.getStart} - end ${t.getTimeSeries.getEnd}")
      })
    }
    chunks.toList
  }

  /**
   * determine grouping criteria depending on number of points
   * If 10 per day, 300 per month, 3000 per year then group by year for those metrics
   * If 10000 per day then group by day for those metrics
   *
   * @param sparkSession
   * @param solrDf
   * @return
   */
  private def mergeChunks(sparkSession: SparkSession, solrDf: DataFrame): Dataset[TimeseriesRecord] = {
    val dailyChunks: Dataset[TimeseriesRecord] = chunkDailyMetrics(solrDf)
    dailyChunks
  }

  private def chunkDailyMetrics(solrDf: DataFrame): Dataset[TimeseriesRecord] = {
    val groupedChunkDf = solrDf
      .groupBy(
        col(TimeseriesRecord.METRIC_NAME),
        col(TimeseriesRecord.CHUNK_YEAR),
        col(TimeseriesRecord.CHUNK_MONTH),
        col(TimeseriesRecord.CHUNK_DAY))
      .agg(
        sum(col(TimeseriesRecord.CHUNK_SIZE)).as(totalNumberOfPointColumnName),
        f.collect_list(col(TimeseriesRecord.CHUNK_START)).as(startsColumnName),
        f.collect_list(col(TimeseriesRecord.CHUNK_VALUE)).as(valuesColumnName),
        f.collect_list(col(TimeseriesRecord.CHUNK_END)).as(endsColumnName),
        f.collect_list(col(TimeseriesRecord.CHUNK_SIZE)).as(sizesColumnName),
        f.first(col(TimeseriesRecord.METRIC_NAME)).as(nameColumnName)
      )

    import groupedChunkDf.sparkSession.implicits._

    groupedChunkDf
      .rdd
      .flatMap(mergeChunksIntoSeveralChunk)
      .map(calculMetrics)
      .toDS()
  }

  private def calculMetrics(timeSerie: TimeseriesRecord): TimeseriesRecord = {
    // Init the Timeserie processor
    val tsProcessor = new TimeseriesConverter()
    val context = new HistorianContext(tsProcessor)
    context.setProperty(TimeseriesConverter.GROUPBY.getName, TimeseriesRecord.METRIC_NAME)
    context.setProperty(TimeseriesConverter.METRIC.getName,
      s"first;min;max;count;sum;avg;count;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")
    tsProcessor.init(context)
    tsProcessor.computeValue(timeSerie)
    tsProcessor.computeMetrics(timeSerie)
    EvoaUtils.setBusinessFields(timeSerie)
    EvoaUtils.setDateFields(timeSerie)
    EvoaUtils.setHashId(timeSerie)
    EvoaUtils.setChunkOrigin(timeSerie, jobId)
    timeSerie
  }

  private def mergeChunksIntoSeveralChunk(r: Row): List[TimeseriesRecord] = {
    val name = r.getAs[String](nameColumnName)
    val values = r.getAs[ArrayDF[String]](valuesColumnName)
    val starts = r.getAs[ArrayDF[Long]](startsColumnName)
    val ends = r.getAs[ArrayDF[Long]](endsColumnName)
    val sizes = r.getAs[ArrayDF[Long]](sizesColumnName)
    val totalPoints = r.getAs[Long](totalNumberOfPointColumnName)
    logger.trace(s"A total of points of $totalPoints")
    val chunked = chunkIntoSeveralTimeseriesRecord("evoa_measure", name, values, starts, ends, sizes)
    chunked
  }

  /**
   * save job report
   * @param solrChunks
   * @return
   */
  private def saveReportJobAfterTagging(solrChunks: DataFrame) = {
    val start = System.currentTimeMillis()
    logger.info(s"Saving report after tagging finished of job $jobId to ${options.reportCollectionName}")
    val reportDoc = solrChunks.select(
      f.col(TimeseriesRecord.METRIC_NAME)
    )
      .agg(
        f.lit(jobId).as(CompactorJobReport.JOB_ID),
        f.lit(CompactorJobReport.JOB_TYPE_VALUE).as(CompactorJobReport.JOB_TYPE),
        f.lit(jobStart).as(CompactorJobReport.JOB_START),
        f.lit(JobStatus.RUNNING.toString).as(CompactorJobReport.JOB_STATUS),
        f.count(TimeseriesRecord.METRIC_NAME).as(CompactorJobReport.JOB_NUMBER_OF_CHUNK_INPUT),
        f.countDistinct(TimeseriesRecord.METRIC_NAME).as(CompactorJobReport.JOB_TOTAL_METRICS_RECHUNKED),
        f.lit(options.toJsonStr).as(CompactorJobReport.JOB_CONF)
      )
    reportDoc.write
      .format("solr")
      .options(Map(
        "zkhost" -> options.zkHosts,
        "collection" -> options.reportCollectionName
      )).save()

    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.reportCollectionName, true, true)
    logger.info(s"Saving report after tagging finished of job $jobId to ${options.reportCollectionName} :\n{}", response)
    if (logger.isTraceEnabled) (
      reportDoc.show(false)
    )
    logger.info(s"saving report after tagging lasted ${(start - System.currentTimeMillis()) / 1000} seconds")
    reportDoc
  }

  private def buildJobId(start:Long) = {
    val pattern = "MM-dd-yyyy'T'HH:mm:ss.ZZZ"
    val df = new SimpleDateFormat(pattern)
    val date: String =  df.format(new Date(start))
    s"compaction-$date"
  }

  private def saveReportJobSuccess(savedDF: DataFrame) = {
    logger.info(s"start saving success report of job $jobId to ${options.reportCollectionName}")
    val numberOfChunkOutput: Long = savedDF.count()
    val jobEnd = System.currentTimeMillis()
    val updateDoc = new SolrInputDocument
    updateDoc.setField(CompactorJobReport.JOB_ID, jobId)
    updateDoc.setField(CompactorJobReport.JOB_ELAPSED, jobEnd - jobStart)
    updateDoc.setField(CompactorJobReport.JOB_END, jobEnd)
    updateDoc.setField(CompactorJobReport.JOB_NUMBER_OF_CHUNK_OUTPUT, numberOfChunkOutput)
    updateDoc.setField(CompactorJobReport.JOB_STATUS, new util.HashMap[String, String](1) {
      {
        put("set", JobStatus.SUCCEEDED.toString);
      }
    })

    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val rsp = solrCloudClient.add(options.reportCollectionName, updateDoc)
    handleSolrResponse(rsp)
    val rsp2 = solrCloudClient.commit(options.reportCollectionName, true, true)
    handleSolrResponse(rsp2)
  }


  private def tagChunksToBeCompacted(sparkSession: SparkSession) = {
    val start = System.currentTimeMillis()
    val solrChunks: DataFrame = loadChunksToBeTaggegFromSolR(sparkSession)
    logger.info(s"start tagging chunks to be compacted by job '$jobId' to collection ${options.timeseriesCollectionName}")
    import solrChunks.sparkSession.implicits._
    val chunkIds: Dataset[String] = solrChunks.select(
      f.col(TimeseriesRecord.CHUNK_ID)
    ).as[String]

    val taggedChunks: RDD[SolrInputDocument] = buildTaggegSolrDocRdd(chunkIds)
    HurenceSolrSupport.indexDocs(
      options.zkHosts,
      options.timeseriesCollectionName,
      5000,
      taggedChunks
    )

    logger.info(s"done tagging chunks to be compacted by job '$jobId' to collection ${options.timeseriesCollectionName}")
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.timeseriesCollectionName, true, true)
    handleSolrResponse(response)
    logger.info(s"Tagging lasted ${(start - System.currentTimeMillis()) / 1000} seconds")
  }

  /**
   *
   * @param chunkIds containing ids of doc to tag
   * @return
   */
  private def buildTaggegSolrDocRdd(chunkIds: Dataset[String]) = {
    val addJobIdTag: util.Map[String, String] = new util.HashMap[String, String](1) {
      {
        put("add", jobId);
      }
    }
    chunkIds.rdd.map(id => {
      val updateDoc = new SolrInputDocument
      updateDoc.setField(TimeseriesRecord.CHUNK_ID, id)
      updateDoc.setField(TimeseriesRecord.CHUNK_COMPACTION_RUNNING, addJobIdTag)
      updateDoc
    })
    //(Encoders.bean(classOf[SolrInputDocument]))
  }

  private def saveReportJobStarting() = {
    val start = System.currentTimeMillis()
    logger.info(s"Saving report that job $jobId started (to collection ${options.reportCollectionName})")
    val reportDoc = new SolrInputDocument
    reportDoc.setField(CompactorJobReport.JOB_ID, jobId)
    reportDoc.setField(CompactorJobReport.JOB_TYPE, CompactorJobReport.JOB_TYPE_VALUE)
    reportDoc.setField(CompactorJobReport.JOB_CONF, options.toJsonStr)
    reportDoc.setField(CompactorJobReport.JOB_STATUS, JobStatus.RUNNING.toString)
    reportDoc.setField(CompactorJobReport.JOB_START, jobStart)
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val rsp = solrCloudClient.add(options.reportCollectionName, reportDoc)
    handleSolrResponse(rsp)
    val rsp2 = solrCloudClient.commit(options.reportCollectionName, true, true)
    handleSolrResponse(rsp2)
    logger.info(s"Start report writing lasted ${(start - System.currentTimeMillis()) / 1000} seconds")
  }

  private def handleSolrResponse(response: UpdateResponse) = {
    logger.info(s"response : \n{}", response)
    val statusCode = response.getStatus
    if (statusCode == 0) {
      logger.info(s"request succeeded, elapsed time is {},\n header : \n${response.getResponseHeader}\n body ${response.getResponse}", response.getElapsedTime)
    } else {
      logger.error(s"error during in response, header : \n${response.getResponseHeader}\n body ${response.getResponse}")
      throw response.getException
    }
  }

  private def deleteTaggedChunks() = {
    val query = s"""${TimeseriesRecord.CHUNK_COMPACTION_RUNNING}:"$jobId""""
    deleteByQuery(options.timeseriesCollectionName, options.zkHosts, query)
  }

  def deleteByQuery(collectionName: String, zkHost: String, query: String) = {
    val start = System.currentTimeMillis()
    val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
    logger.info(s"will permanently delete docs matching $query from ${collectionName}}")
    val rsp = solrCloudClient.deleteByQuery(collectionName, query)
    handleSolrResponse(rsp)
    val rsp2 = solrCloudClient.commit(collectionName, true, true)
    handleSolrResponse(rsp2)
    logger.info(s"Deleting data from collection $collectionName with query $query took ${(start - System.currentTimeMillis()) / 1000} seconds")
  }

  private def saveReportJobFailed(stageOfFailure: String, ex: Throwable) = {
    logger.info(s"start saving success report of job $jobId to ${options.reportCollectionName}")
    val jobEnd = System.currentTimeMillis()
    val updateDoc = new SolrInputDocument
    updateDoc.setField(CompactorJobReport.JOB_ID, jobId)
    updateDoc.setField(CompactorJobReport.JOB_ELAPSED, jobEnd - jobStart)
    updateDoc.setField(CompactorJobReport.JOB_END, jobEnd)
    updateDoc.setField(CompactorJobReport.JOB_ERROR, stageOfFailure)
    updateDoc.setField(CompactorJobReport.JOB_EXCEPTION_MSG, ex.getMessage)
    updateDoc.setField(CompactorJobReport.JOB_STATUS, new util.HashMap[String, String](1) {
      {
        put("set", JobStatus.FAILED.toString);
      }
    })

    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val rsp = solrCloudClient.add(options.reportCollectionName, updateDoc)
    handleSolrResponse(rsp)
    val rsp2 = solrCloudClient.commit(options.reportCollectionName, true, true)
    handleSolrResponse(rsp2)
  }

  /**
   * Need to eventually delete chunks that would have been successfully injected when an error occurs
   */
  private def deleteCompactedChunks() = {
    val query = s"""${TimeseriesRecord.CHUNK_ORIGIN}:"$jobId""""
    deleteByQuery(options.timeseriesCollectionName, options.zkHosts, query)
  }
  /**
   * Compact chunks of historian
   */
  override def run(spark: SparkSession): Unit = {
    saveReportJobStarting()
    val timeseriesDS: DataFrame = calculMetricsInReportAndLoadTimeSeries(spark)
    val savedDF: DataFrame = compactThenSaveChunks(spark, timeseriesDS)
    if (options.useCache) {
      timeseriesDS.unpersist()
    }
    deleteOldChunks
    saveSuccessReport(savedDF)
  }

  private def saveSuccessReport(savedDF: DataFrame) = {
    try {
      saveReportJobSuccess(savedDF)
    } catch {
      case ex: Throwable => {
        logger.error("Failed during tagging", ex)
        saveReportJobFailed("Failed while attempting to write final report after compactor finished smoothly", ex)
        throw ex
      }
    }
  }

  def deleteChunksWithQuery() = {

  }

  private def deleteOldChunks = {
    if (options.tagging) {
      try {
        //TODO check chunked data equal not chunked data ? (so we are sure of not loosing data) Maybe a count of points for each metrics (would be a first verif)
        deleteTaggedChunks()
      } catch {
        case ex: Throwable => {
          logger.error("Failed during tagging", ex)
          saveReportJobFailed("Error happened while deleting tagged chunks, so there may be duplicates in datas ! " +
            "Be sure to clean chunks before re running a compaction", ex)
          throw ex
        }
      }
    } else {
      //This is unsafe !!!! Use this carefully in prod. The query could match newly injected chunks that are not yet chunked.
      //This means that these chunks would be deleted (So there would be a lost of data)
      deleteByQuery(options.timeseriesCollectionName, options.zkHosts, options.solrFq)
    }
  }


  def calculMetricsInReportAndLoadTimeSeries(spark: SparkSession): DataFrame = {
    if (options.tagging) {
      tagChunksThenCalculateMetricsInReportThenLoadTaggedChunks(spark)
    } else {
      loadChunksWithQueryThenCalculMetricsInReportThenReturnLoadedChunks(spark)
    }
  }

  private def loadChunksWithQueryThenCalculMetricsInReportThenReturnLoadedChunks(spark: SparkSession) = {
    val timeseriesDS = loadChunksFromSolR(spark)
    if (options.useCache) {
      timeseriesDS.cache()
    }
    try {
      saveReportJobAfterTagging(timeseriesDS)
      timeseriesDS
    } catch {
      case ex: Throwable => {
        logger.error("Failed during tagging", ex)
        saveReportJobFailed("Failed while attempting to write intermediary report after chunks have been tagged", ex)
        throw ex
      }
    }
  }

  private def tagChunksThenCalculateMetricsInReportThenLoadTaggedChunks(spark: SparkSession) = {
    try {
      tagChunksToBeCompacted(spark)
    } catch {
      case ex: Throwable => {
        logger.error("Failed during tagging", ex)
        saveReportJobFailed("Failed while tagging chunks to be compacted", ex)
        throw ex
      }
    }
    val timeseriesDS = loadTaggedChunksFromSolR(spark)
    if (options.useCache) {
      timeseriesDS.cache()
    }
    try {
      saveReportJobAfterTagging(timeseriesDS)
      timeseriesDS
    } catch {
      case ex: Throwable => {
        logger.error("Failed during tagging", ex)
        saveReportJobFailed("Failed while attempting to write intermediary report after chunks have been tagged", ex)
        throw ex
      }
    }
  }

  def compactThenSaveChunks(spark: SparkSession, timeseriesDS: DataFrame) = {
    try {
      val mergedTimeseriesDS = mergeChunks(sparkSession = spark, timeseriesDS)
      saveNewChunksToSolR(mergedTimeseriesDS)
    } catch {
      case ex: Throwable => {
        logger.error("Failed during tagging", ex)
        saveReportJobFailed("Failed while trying to save newly compacted chunks", ex)
        deleteCompactedChunks()
        throw ex
      }
    }
  }
}
