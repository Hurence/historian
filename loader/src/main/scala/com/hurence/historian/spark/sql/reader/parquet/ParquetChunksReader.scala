package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

class ParquetChunksReader extends Reader[ChunkVersionCurrent] {


  override def read(options: Options): Dataset[ChunkVersionCurrent] = {


    val spark = SparkSession.getActiveSession.get
   // implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[ChunkRecordV0]

    spark.read.parquet(options.path)
     // .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .as[ChunkVersionCurrent](Encoders.bean(classOf[ChunkVersionCurrent]))
  }
}
