package com.hurence.historian

import com.hurence.historian.ChunkCompactorJob.ChunkCompactorConf
import org.junit.jupiter.api.Disabled
import org.testcontainers.containers.DockerComposeContainer

@Disabled("Currently not working. A bug that include some chunks of size 0")
class ReducingChunkSizeStrategy1Test(container: (DockerComposeContainer[SELF]) forSome {type SELF <: DockerComposeContainer[SELF]})
  extends AbstractReducingChunkSizeTest(container) {

  override def getCompactorFactory() = {
    (conf: ChunkCompactorConf) => new ChunkCompactorJobStrategy1(conf)
  }
}
