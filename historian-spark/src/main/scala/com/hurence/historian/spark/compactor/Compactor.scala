package com.hurence.historian.spark.compactor


import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.hurence.historian.date.util.DateUtil
import com.hurence.historian.spark.ml.{Chunkyfier, UnChunkyfier}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, ReaderFactory}
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.hurence.timeseries.model.Definitions._
import com.hurence.timeseries.model.{Chunk, Measure}
import com.lucidworks.spark.util.SolrSupport
import org.apache.log4j.{Level, LogManager}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.{SolrClient, SolrQuery}
import org.apache.spark.sql.{SparkSession, _}

import scala.collection.mutable


class Compactor(val options: CompactorConf) extends Serializable with Runnable {

  private val logger = LogManager.getLogger(classOf[Compactor])
  private var solrClient: CloudSolrClient = null
  private var spark: SparkSession = null
  private var scheduledThreadPool: ScheduledExecutorService = null
  private var started = false
  private var closed = false

  def start(): Unit = {
    if (closed) throw new IllegalStateException("Cannot call start on closed compactor")
    if (!started) {
      logger.info("Starting compactor using configuration: " + options)

      spark = SparkSession.builder
        .appName(options.spark.appName)
        .master(options.spark.master)
        .config("spark.sql.shuffle.partitions", options.spark.sqlShufflePartitions)
        .getOrCreate()

      solrClient = SolrSupport.getCachedCloudClient(options.solr.zkHosts)


      /**
        * Start looping running the compaction algorithm
        */
      // Need only one thread running the compaction
      scheduledThreadPool = Executors.newScheduledThreadPool(1)
      // Will the first algorithm run now of after a delay?
      var delay = options.scheduler.period // First run will start in period seconds from now
      if (options.scheduler.startNow) delay = 0 // First run will occur now
      logger.info("Compactor starting in " + delay + " seconds and then every " + options.scheduler.period + " seconds")
      scheduledThreadPool.scheduleWithFixedDelay(this, delay, options.scheduler.period, TimeUnit.SECONDS)
      started = true
      logger.info("Compactor started")
    } else {
      logger.warn("Attempt to start compactor while already started, doing nothing")
    }

  }

  def stop(): Unit = {
    if (closed) throw new IllegalStateException("Cannot call stop on closed compactor")
    if (!started) {
      logger.info("Attempt to stop compactor while already stopped, doing nothing")
      return
    }
    logger.info("Stopping compactor")
    scheduledThreadPool.shutdown()
    try scheduledThreadPool.awaitTermination(3600, TimeUnit.SECONDS)
    catch {
      case e: InterruptedException =>
        logger.error("Error while waiting for compactor to stop: " + e.getMessage)
        return
    }

    logger.info("Closing compactor")

    spark.close()
    solrClient.close()
    closed = true
    started = false
    logger.info("Compactor stopped")
  }

  override def run(): Unit = {

    if (closed) throw new IllegalStateException("Cannot call run on closed compactor")

    try {
      findDaysToCompact()
        .foreach(day => {
          val uncompactedChunks = loadChunksFromSolr(day)
          val measuresDS = convertChunksToMeasures(uncompactedChunks)
          val compactedChunksDS = convertMeasuresToChunks(measuresDS)
          writeCompactedChunksToSolr(compactedChunksDS)
          if (checkChunksIntegrity(compactedChunksDS))
            deleteOldChunks(day)

          /*
            curl http://localhost:8983/solr/historian/update -H "Content-type: text/xml" --data-binary '<delete><query>chunk_origin:compactor*it*</query></delete>'
            curl http://localhost:8983/solr/historian/update -H "Content-type: text/xml" --data-binary '<commit />'



           */
        })
    }
    catch {
      case t: Throwable =>
        logger.error("Error running compaction algorithm", t)
    }


  }

  /**
    * Get old documents whose chunk origin is not compactor, starting from yesterday
    *
    * Prepare query that gets documents (and operator):
    * - from epoch since yesterday (included)
    * - with origin not from compactor (chunks not already compacted and thus needing to be)
    * Return only interesting fields:
    * - metric_key
    * - chunk_day
    * Documentation for query parameters of spark-solr: https://github.com/lucidworks/spark-solr#query-parameters
    * Example:
    *
    * chunk_start:[* TO 1600387199999]
    * AND -chunk_origin:compactor
    * fields metric_key,chunk_day
    * rows 1000
    * request_handler /export
    *
    * @return the list of days formatted as a string like "yyyy-MM-dd"
    */
  def findDaysToCompact() = {
    val days = mutable.HashSet[String]()

    // build SolR facet query
    val solrQuery = new SolrQuery("*:*")
    val filterQuery = if (options.reader.queryFilters.isEmpty)
      s"$SOLR_COLUMN_START :[* TO ${DateUtil.utcFirstTimestampOfTodayMs - 1L} ]"
    else
      s"$SOLR_COLUMN_START :[* TO ${DateUtil.utcFirstTimestampOfTodayMs - 1L} ] AND (${options.reader.queryFilters})"

    solrQuery.addFilterQuery(filterQuery)
    solrQuery.setRows(0)
    solrQuery.addFacetField(SOLR_COLUMN_DAY)
    solrQuery.setFacet(true)
    solrQuery.setFacetMinCount(1)
    logger.debug(s"looking for days to compact : fq=$filterQuery")

    // run that query and convert response to a set of days
    val response = solrClient.query(options.solr.collectionName, solrQuery)
    import scala.collection.JavaConversions._
    for (d <- response.getFacetField(SOLR_COLUMN_DAY).getValues) {
      days.add(d.getName)
    }

    if (days.isEmpty)
      logger.debug("no chunk found for compaction")
    else
      logger.debug("found " + days.toList.mkString(",") + " to be compacted")
    days
  }

  /**
    * get all chunks from the day
    *
    * @TODO can be optimized to remove all those that does not need any compaction
    * @return the chunks dataset
    */
  def loadChunksFromSolr(day: String) = {
    logger.debug(s"start loading chunks from SolR collection : ${options.solr.collectionName} for day $day")

    val filterQuery = if (options.reader.queryFilters.isEmpty)
      s"$SOLR_COLUMN_DAY:$day"
    else
      s"$SOLR_COLUMN_DAY:$day AND (${options.reader.queryFilters})"

    ReaderFactory.getChunksReader(ChunksReaderType.SOLR)
      .read(sql.Options(
        options.solr.collectionName,
        Map(
          "zkhost" -> options.solr.zkHosts,
          "collection" -> options.solr.collectionName,
          "tag_names" -> options.reader.tagNames,
          "filters" -> filterQuery
        )))
      .as[Chunk](Encoders.bean(classOf[Chunk]))
  }

  /**
    * convert a dataset of chunks into a dataset of measures
    *
    * @param chunksDS
    * @return the dataset of measures
    */
  def convertChunksToMeasures(chunksDS: Dataset[Chunk]): Dataset[Measure] = {
    new UnChunkyfier().transform(chunksDS)
      .as[Measure](Encoders.bean(classOf[Measure]))
  }

  /**
    * transform Measures into Chunks
    *
    * @param measuresDS inpout dataset
    * @return chunk dataset
    */
  def convertMeasuresToChunks(measuresDS: Dataset[Measure]): Dataset[Chunk] = {
    new Chunkyfier()
      .setOrigin(options.chunkyfier.origin)
      .setGroupByCols(options.chunkyfier.groupByCols.split(","))
      .setDateBucketFormat(options.chunkyfier.dateBucketFormat)
      .setSaxAlphabetSize(options.chunkyfier.saxAlphabetSize)
      .setSaxStringLength(options.chunkyfier.saxStringLength)
      .transform(measuresDS)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
  }

  /**
    * save the new new chunks to SolR
    *
    * @param chunksDS
    */
  def writeCompactedChunksToSolr(chunksDS: Dataset[Chunk]) = {
    // write chunks to SolR
    WriterFactory.getChunksWriter(WriterType.SOLR)
      .write(sql.Options(options.solr.collectionName, Map(
        "zkhost" -> options.solr.zkHosts,
        "collection" -> options.solr.collectionName,
        "tag_names" -> options.reader.tagNames
      )), chunksDS)

    // explicit commit to make sure all docs are immediately visible
    val response = solrClient.commit(options.solr.collectionName, true, true)
    logger.debug(s"done saving new chunks to collection ${options.solr.collectionName}")

    chunksDS
  }


  /**
    *
    * @return
    */
  def checkChunksIntegrity(chunksDS: Dataset[Chunk]) = {
    val q = new SolrQuery("*:*")
    val response = solrClient.query(options.solr.collectionName, q)

    true
  }

  /**
    * remove all previous chunks
    *
    * @return
    */
  def deleteOldChunks(day: String) = {
    val solrQuery = s"$SOLR_COLUMN_DAY:$day AND -$SOLR_COLUMN_ORIGIN:${options.chunkyfier.origin}"
    logger.debug(s"will delete by query  q=$solrQuery")

    try solrClient.deleteByQuery(options.solr.collectionName, solrQuery)
    catch {
      case e: Exception =>
        logger.error(s"Error deleting chunk documents q=$solrQuery : ${e.getMessage}")
    }


    try {
      logger.debug("Committing documents deletion")
      solrClient.commit(options.solr.collectionName, true, true)
    } catch {
      case e: Exception =>
        logger.error("Error committing deleted chunks: " + e.getMessage)
    }
  }

}

object Compactor {


  /**
    *
    * $SPARK_HOME/bin/spark-submit --driver-memory 4g --driver-java-options '-Dlog4j.configuration=file:historian-resources/conf/log4j.properties' --class  com.hurence.historian.spark.compactor.Compactor --jars  historian-resources/jars/spark-solr-3.6.6-shaded.jar,historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar  historian-spark/target/historian-spark-1.3.6-SNAPSHOT.jar --config-file historian-resources/conf/compactor-config.yaml
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // get arguments & setup spark session
    val options = if (args.size == 0)
      ConfigLoader.defaults()
    else
      ConfigLoader.loadFromFile(args(1))

    // start compaction
    val compactor = new Compactor(options)
    compactor.start()
  }


}
