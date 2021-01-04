package com.hurence.historian.spark.sql.reader.parquet

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.functions.reorderColumns
import com.hurence.historian.spark.sql.reader.{Reader, ReaderFactory}
import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class ParquetMeasuresReader extends Reader[Measure] {


  override def read(options: Options): Dataset[Measure] = {


    val spark = SparkSession.getActiveSession.get


    implicit val measureEncoder = Encoders.bean(classOf[Measure])



    spark.read
      .parquet(options.path)
      .map(r => Measure.builder()
        .name(r.getAs[String]("name"))
        .timestamp(r.getAs[Long]("timestamp"))
        .value(r.getAs[Double]("value"))
        .tags(r.getAs[Map[String, String]]("tags").asJava)
        .build()
      )
      .as[Measure]
  }
}
