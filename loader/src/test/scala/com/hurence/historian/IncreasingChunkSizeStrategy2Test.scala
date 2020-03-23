package com.hurence.historian

import com.hurence.historian.modele.CompactorJobReport
import com.hurence.logisland.record.TimeSeriesRecord
import io.vertx.core.json.JsonObject
import org.junit.jupiter.api.Assertions.assertEquals
import org.testcontainers.containers.DockerComposeContainer

class IncreasingChunkSizeStrategy2Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractIncreasingChunkSizeTest(container) {

  val compactorConf: ChunkCompactorConfStrategy2 = ChunkCompactorConfStrategy2(zkUrl, historianCollection,
    CompactorJobReport.DEFAULT_COLLECTION,
    chunkSize = 10,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day,
    solrFq = s"${TimeSeriesRecord.CHUNK_ORIGIN}:logisland")

  override def createCompactor() = {
    new ChunkCompactorJobStrategy2(compactorConf)
  }

  override def additionalTestsOnReportEnd(report: JsonObject): Unit = {
    assertEquals(compactorConf.toJsonStr, report.getString(CompactorJobReport.JOB_CONF))
  }
}
