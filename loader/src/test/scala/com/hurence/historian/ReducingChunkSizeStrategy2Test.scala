package com.hurence.historian

import com.hurence.historian.modele.CompactorJobReport
import com.hurence.logisland.record.TimeSeriesRecord
import org.testcontainers.containers.DockerComposeContainer

class ReducingChunkSizeStrategy2Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractReducingChunkSizeTest(container) {

  val compactorConf: ChunkCompactorConfStrategy2 = ChunkCompactorConfStrategy2(zkUrl, historianCollection,
    CompactorJobReport.DEFAULT_COLLECTION,
    chunkSize = chunkSize,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day,
    solrFq = s"${TimeSeriesRecord.CHUNK_ORIGIN}:logisland")

  override def createCompactor() = {
    new ChunkCompactorJobStrategy2(compactorConf)
  }
}
