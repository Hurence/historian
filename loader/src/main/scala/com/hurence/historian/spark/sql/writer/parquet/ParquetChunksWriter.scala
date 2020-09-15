package com.hurence.historian.spark.sql.writer.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.writer.Writer
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent
import org.apache.spark.sql.Dataset

class ParquetChunksWriter extends Writer[ChunkVersionCurrent] {

  override def write(options: Options, ds: Dataset[_ <: ChunkVersionCurrent]) = {
    ds.write
      .partitionBy("day")
      .mode("append")
      .parquet(options.path)
  }

}
