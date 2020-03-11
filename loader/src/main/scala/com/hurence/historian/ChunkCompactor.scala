package com.hurence.historian

import com.hurence.logisland.record.Point

case class CompactionChunkInfo(value: String, start: Long, end: Long, size: Long)

class ChunkCompactor() {




  def getPoints(): Stream[Point] = {
    List().toStream
  }


  def addChunk(c: CompactionChunkInfo): Unit = {

  }
}
