package com.hurence.historian.spark.sql.writer.parquet

import com.hurence.historian.modele.ChunkRecordV0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.writer.Writer
import org.apache.spark.sql.Dataset

class ParquetChunksWriter extends Writer[ChunkRecordV0] {

  override def write(options: Options, ds: Dataset[ChunkRecordV0]) = {
    ds.write
      .partitionBy("day")
      .mode("append")
      .parquet(options.path)
  }

}
