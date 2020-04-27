package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.model.ChunkRecordV0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import org.apache.spark.sql.Dataset

class SolrChunksReader extends Reader[ChunkRecordV0] {

  override def read(options: Options): Dataset[ChunkRecordV0] = {
   null
  }

}
