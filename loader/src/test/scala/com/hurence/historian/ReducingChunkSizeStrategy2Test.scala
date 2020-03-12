package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import org.testcontainers.containers.DockerComposeContainer

class ReducingChunkSizeStrategy2Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractReducingChunkSizeTest(container) {

  override def getCompactorFactory() = {
    (conf: ChunkCompactorConf) => new ChunkCompactorJobStrategy2(conf)
  }
}
