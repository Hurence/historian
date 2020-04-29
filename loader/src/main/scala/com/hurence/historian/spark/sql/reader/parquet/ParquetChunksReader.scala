package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.model.ChunkRecordV0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class ParquetChunksReader extends Reader[ChunkRecordV0] {


  override def read(options: Options): Dataset[ChunkRecordV0] = {


    val spark = SparkSession.getActiveSession.get

    import spark.implicits._
   // implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[ChunkRecordV0]

    spark.read.parquet(options.path)
     // .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .as[ChunkRecordV0]
  }
}