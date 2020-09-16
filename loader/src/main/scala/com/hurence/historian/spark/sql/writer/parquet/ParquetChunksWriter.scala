package com.hurence.historian.spark.sql.writer.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.writer.Writer
import com.hurence.timeseries.model.Chunk
import org.apache.spark.sql.Dataset

class ParquetChunksWriter extends Writer[Chunk] {

  override def write(options: Options, ds: Dataset[_ <: Chunk]) = {
    ds.write
      .partitionBy("day")
      .mode("append")
      .parquet(options.path)
  }

}
