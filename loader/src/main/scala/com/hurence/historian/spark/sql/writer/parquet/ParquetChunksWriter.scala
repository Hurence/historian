package com.hurence.historian.spark.sql.writer.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.writer.Writer
import com.hurence.timeseries.modele.chunk.ChunkVersion0
import org.apache.spark.sql.Dataset

class ParquetChunksWriter extends Writer[ChunkVersion0] {

  override def write(options: Options, ds: Dataset[_ <: ChunkVersion0]) = {
    ds.write
      .partitionBy("day")
      .mode("append")
      .parquet(options.path)
  }

}
