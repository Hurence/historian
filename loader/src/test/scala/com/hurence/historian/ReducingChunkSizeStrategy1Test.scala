package com.hurence.historian

import org.junit.jupiter.api.Disabled
import org.testcontainers.containers.DockerComposeContainer

@Disabled("Currently not working. A bug that include some chunks of size 0")
class ReducingChunkSizeStrategy1Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractReducingChunkSizeTest(container) {

  val compactorConf: ChunkCompactorConf = ChunkCompactorConf(zkUrl, historianCollection,
    chunkSize = chunkSize,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day)

  override def createCompactor() = {
    new ChunkCompactorJobStrategy1(compactorConf)
  }
}
