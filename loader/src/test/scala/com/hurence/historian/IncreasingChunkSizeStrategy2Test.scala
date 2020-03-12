package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import org.testcontainers.containers.DockerComposeContainer

class IncreasingChunkSizeStrategy2Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractIncreasingChunkSizeTest(container) {

  override def getCompactorFactory() = {
    (conf: ChunkCompactorConf) => new ChunkCompactorJobStrategy2(conf)
  }
}
