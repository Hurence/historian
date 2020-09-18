package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.model.{Chunk, Measure}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class ParquetChunksReader extends Reader[Chunk] {


  override def read(options: Options): Dataset[Chunk] = {


    val spark = SparkSession.getActiveSession.get

    implicit val encoder = Encoders.bean(classOf[Chunk])

    spark.read.parquet(options.path)
      .map(r => {
        Chunk.builder()
          .name(r.getAs[String]("name"))
          .start(r.getAs[Long]("start"))
          .end(r.getAs[Long]("end"))
          .count(r.getAs[Long]("count"))
          .avg(r.getAs[Double]("avg"))
          .std(r.getAs[Double]("stddev"))
          .min(r.getAs[Double]("min"))
          .max(r.getAs[Double]("max"))
          .chunkOrigin("it-data-4metrics-chunk.parquet")
          .first(r.getAs[Double]("first"))
          .last(r.getAs[Double]("last"))
          .sax(r.getAs[String]("sax"))
          .valueBinaries(BinaryEncodingUtils.decode(r.getAs[String]("chunk")))
          .tags(r.getAs[Map[String, String]]("tags").asJava)
          // .buildId()
          .computeMetrics()
          .build()
      })
      .as[Chunk]
  }
}
