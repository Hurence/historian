package com.hurence.historian

import com.hurence.historian.spark.compactor.{ChunkCompactorConfStrategy2, ChunkCompactorJobStrategy2SchemaVersion0}
import com.hurence.historian.spark.compactor.job.CompactorJobReport
import com.hurence.logisland.record.TimeSeriesRecord
import io.vertx.core.json.JsonObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.testcontainers.containers.DockerComposeContainer

class IncreasingChunkSizeStrategy2IT(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractIncreasingChunkSizeIT(container) {

  val compactorConf: ChunkCompactorConfStrategy2 = ChunkCompactorConfStrategy2(zkUrl, historianCollection,
    CompactorJobReport.DEFAULT_COLLECTION,
    chunkSize = 10,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    solrFq = s"${TimeSeriesRecord.CHUNK_ORIGIN}:logisland",
    false,
    true
  )

  override def createCompactor() = {
    new ChunkCompactorJobStrategy2SchemaVersion0(compactorConf)
  }

  override def additionalTestsOnReportEnd(report: JsonObject): Unit = {
    assertEquals(compactorConf.toJsonStr, report.getString(CompactorJobReport.JOB_CONF))
  }
}
