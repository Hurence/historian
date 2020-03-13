package com.hurence.historian

import org.testcontainers.containers.DockerComposeContainer

class ReducingChunkSizeStrategy2Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractReducingChunkSizeTest(container) {

  val compactorConf: ChunkCompactorConfStrategy2 = ChunkCompactorConfStrategy2(zkUrl, historianCollection,
    chunkSize = chunkSize,
    saxAlphabetSize = 2,
    saxStringLength = 3,
    year = year,
    month = month,
    day = day,
    "logisland")

  override def createCompactor() = {
    new ChunkCompactorJobStrategy2(compactorConf)
  }
}
