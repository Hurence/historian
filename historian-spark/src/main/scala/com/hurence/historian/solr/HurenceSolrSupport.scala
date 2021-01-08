package com.hurence.historian.solr

import java.net.{ConnectException, SocketException}

import com.lucidworks.spark.util.{CacheCloudSolrClient, SolrSupport}
import com.lucidworks.spark.SparkSolrAccumulator
import org.apache.http.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.spark.rdd.RDD
import org.apache.zookeeper.KeeperException.{OperationTimeoutException, SessionExpiredException}

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.mutable.ArrayBuffer

object HurenceSolrSupport extends LazyLogging {

  def indexDocs(
                 zkHost: String,
                 collection: String,
                 batchSize: Int,
                 rdd: RDD[SolrInputDocument]): Unit = indexDocs(zkHost, collection, batchSize, rdd, None)

  def indexDocs(
                 zkHost: String,
                 collection: String,
                 batchSize: Int,
                 rdd: RDD[SolrInputDocument],
                 commitWithin: Option[Int],
                 accumulator: Option[SparkSolrAccumulator] = None): Unit = {
    //TODO: Return success or false by boolean ?
    rdd.foreachPartition(solrInputDocumentIterator => {
      val solrClient = SolrSupport.getCachedCloudClient(zkHost)
      val batch = new ArrayBuffer[SolrInputDocument]()
      var numDocs: Long = 0
      while (solrInputDocumentIterator.hasNext) {
        val doc = solrInputDocumentIterator.next()
        batch += doc
        if (batch.length >= batchSize) {
          numDocs += batch.length
          if (accumulator.isDefined)
            accumulator.get.add(batch.length.toLong)
          sendBatchToSolrWithRetry(zkHost, solrClient, collection, batch, commitWithin)
          batch.clear
        }
      }
      if (batch.nonEmpty) {
        numDocs += batch.length
        if (accumulator.isDefined)
          accumulator.get.add(batch.length.toLong)
        sendBatchToSolrWithRetry(zkHost, solrClient, collection, batch, commitWithin)
        batch.clear
      }
    })
  }

  def sendBatchToSolrWithRetry(
                                zkHost: String,
                                solrClient: SolrClient,
                                collection: String,
                                batch: Iterable[SolrInputDocument],
                                commitWithin: Option[Int]): Unit = {
    try {
      sendBatchToSolr(solrClient, collection, batch, commitWithin)
    } catch {
      // Reset the cache when SessionExpiredException is thrown. Plus side is that the job won't fail
      case e : Exception =>
        SolrException.getRootCause(e) match {
          case e1 @ (_:SessionExpiredException | _:OperationTimeoutException) =>
            logger.info("Got an exception with message '" + e1.getMessage +  "'.  Resetting the cached solrClient")
            CacheCloudSolrClient.cache.invalidate(zkHost)
            val newClient = SolrSupport.getCachedCloudClient(zkHost)
            sendBatchToSolr(newClient, collection, batch, commitWithin)
        }
    }
  }

  def sendBatchToSolr(solrClient: SolrClient, collection: String, batch: Iterable[SolrInputDocument]): Unit =
    sendBatchToSolr(solrClient, collection, batch, None)

  def sendBatchToSolr(
                       solrClient: SolrClient,
                       collection: String,
                       batch: Iterable[SolrInputDocument],
                       commitWithin: Option[Int]): Unit = {
    val req = new UpdateRequest()
    req.setParam("collection", collection)

    val initialTime = System.currentTimeMillis()

    if (commitWithin.isDefined)
      req.setCommitWithin(commitWithin.get)

    logger.info("Sending batch of " + batch.size + " to collection " + collection)

    req.add(asJavaCollection(batch))

    try {
      solrClient.request(req)
      val timeTaken = (System.currentTimeMillis() - initialTime)/1000.0
      logger.info("Took '" + timeTaken + "' secs to index '" + batch.size + "' documents")
    } catch {
      case e: Exception =>
        if (shouldRetry(e)) {
          logger.error("Send batch to collection " + collection + " failed due to " + e + " ; will retry ...")
          try {
            Thread.sleep(2000)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            solrClient.request(req)
          } catch {
            case ex: Exception =>
              logger.error("Send batch to collection " + collection + " failed due to: " + e, e)
              ex match {
                case re: RuntimeException => throw re
                case e: Exception => throw new RuntimeException(e)
              }
          }
        } else {
          logger.error("Send batch to collection " + collection + " failed due to: " + e, e)
          e match {
            case re: RuntimeException => throw re
            case ex: Exception => throw new RuntimeException(ex)
          }
        }

    }

  }

  def shouldRetry(exc: Exception): Boolean = {
    val rootCause = SolrException.getRootCause(exc)
    rootCause match {
      case e: ConnectException => true
      case e: NoHttpResponseException => true
      case e: SocketException => true
      case _ => false
    }
  }
}
