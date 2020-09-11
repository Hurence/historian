package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.modele.chunk.ChunkVersion0
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

class ParquetChunksReader extends Reader[ChunkVersion0] {


  override def read(options: Options): Dataset[ChunkVersion0] = {


    val spark = SparkSession.getActiveSession.get
   // implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[ChunkRecordV0]

    spark.read.parquet(options.path)
     // .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .as[ChunkVersion0](Encoders.bean(classOf[ChunkVersion0]))
  }
}
