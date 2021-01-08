package com.hurence.historian.spark.sql.writer.solr

import com.google.common.cache._
import com.hurence.historian.service.SolrChunkService
import com.hurence.timeseries.model.Chunk
import org.apache.spark.sql.ForeachWriter

/**
  * a ForeachWriter of spark sql structured streams that sends chunks into solr
  *
  * @param zkHosts
  * @param collectionName
  */
class SolrChunkForeachWriter(zkHosts: String,
                             collectionName: String,
                             numConcurrentRequests: Int,
                             batchSize: Int,
                             flushInterval: Int) extends ForeachWriter[Chunk] {

  override def open(partitionId: Long, version: Long) = true

  override def process(value: Chunk) = {
    CacheSolrChunkService.cache
      .get(ShardInfo(zkHosts, collectionName, numConcurrentRequests, batchSize, flushInterval))
      .put(value)
  }

  override def close(errorOrNull: Throwable) = {}

}


/**
  * SolrChunkService configuration holder class
  *
  * @param zkHosts
  * @param collectionName
  */
case class ShardInfo(zkHosts: String, collectionName: String, numConcurrentRequests: Int, batchSize: Int, flushInterval: Int)


/**
  * a cache for the SolrChunkService, useful to avoid client initialization
  */
object CacheSolrChunkService {

  private val loader = new CacheLoader[ShardInfo, SolrChunkService]() {
    def load(infos: ShardInfo): SolrChunkService = new SolrChunkService(
      infos.zkHosts,
      infos.collectionName,
      infos.numConcurrentRequests,
      infos.batchSize,
      infos.flushInterval
    )
  }

  private val listener = new RemovalListener[ShardInfo, SolrChunkService]() {
    def onRemoval(rn: RemovalNotification[ShardInfo, SolrChunkService]): Unit = {
      if (rn != null && rn.getValue != null) {
        rn.getValue.close()
      }
    }
  }

  val cache: LoadingCache[ShardInfo, SolrChunkService] = CacheBuilder
    .newBuilder()
    .removalListener(listener)
    .build(loader)
}