package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import org.testcontainers.containers.DockerComposeContainer

class IncreasingChunkSizeStrategy1Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractIncreasingChunkSizeTest(container) {

  val compactorConf: ChunkCompactorConf = ChunkCompactorConf(zkUrl, historianCollection,
    chunkSize = 10,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day)

  override def createCompactor() = {
    new ChunkCompactorJobStrategy1(compactorConf)
  }
}