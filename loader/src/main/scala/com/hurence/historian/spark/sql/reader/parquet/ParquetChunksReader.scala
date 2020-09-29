package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class ParquetChunksReader extends Reader[Chunk] {


  override def read(options: Options): Dataset[Chunk] = {


    val spark = SparkSession.getActiveSession.get

    implicit val encoder = Encoders.bean(classOf[Chunk])

    spark.read.parquet(options.path)
      .filter(r => r.getAs[String]("chunk") != null)
      .map(r => {
        Chunk.builder()
          .name(r.getAs[String]("name"))
          .start(r.getAs[Long]("start"))
          .end(r.getAs[Long]("end"))
          .count(r.getAs[Long]("count"))
          .avg(r.getAs[Double]("avg"))
          .stdDev(r.getAs[Double]("std_dev"))
          .min(r.getAs[Double]("min"))
          .max(r.getAs[Double]("max"))
          .first(r.getAs[Double]("first"))
          .last(r.getAs[Double]("last"))
          .sax(r.getAs[String]("sax"))
          .qualityMin(r.getAs[Float]("quality_min"))
          .qualityMax(r.getAs[Float]("quality_max"))
          .qualityFirst(r.getAs[Float]("quality_first"))
          .qualitySum(r.getAs[Float]("quality_sum"))
          .qualityAvg(r.getAs[Float]("quality_avg"))
          .value(BinaryEncodingUtils.decode(r.getAs[String](SOLR_COLUMN_VALUE)))
          //TODO set as byte[] .value(r.getAs[Array[Byte]]($(chunkCol)))
          .tags(r.getAs[Map[String, String]]("tags").asJava)
          .buildId()
          .computeMetrics()
          .build()
      })
      .as[Chunk]
  }
}
