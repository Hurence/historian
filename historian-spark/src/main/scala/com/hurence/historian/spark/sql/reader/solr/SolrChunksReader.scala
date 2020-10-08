package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.functions.fromBase64
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class SolrChunksReader extends Reader[Chunk] {

  override def read(options: Options): Dataset[_ <: Chunk] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get

    implicit val encoder = Encoders.bean(classOf[Chunk])

    val tagNames: List[Column] = options.config(Options.TAG_NAMES)
      .split(",").toList
      .map(tag => col(tag).as(s"tag_$tag"))
    val mainCols = SOLR_COLUMNS.asScala.toList
      .map(name => col(name).as(getFieldFromColumn(name))) ::: tagNames


    val tags: List[Column] = options.config("tag_names")
      .split(",").toList
      .flatMap(tag => List(lit(s"$tag"), col(s"tag_$tag")))

    spark.read
      .format("solr")
      .options(options.config)
      .load()
      .select(mainCols: _*)
      .withColumn(FIELD_VALUE, fromBase64(col(FIELD_VALUE)))
      .withColumn(FIELD_QUALITY_AVG, col(FIELD_QUALITY_AVG).cast(FloatType))
      .withColumn(FIELD_QUALITY_FIRST, col(FIELD_QUALITY_FIRST).cast(FloatType))
      .withColumn(FIELD_QUALITY_MIN, col(FIELD_QUALITY_MIN).cast(FloatType))
      .withColumn(FIELD_QUALITY_MAX, col(FIELD_QUALITY_MAX).cast(FloatType))
      .withColumn(FIELD_QUALITY_SUM, col(FIELD_QUALITY_SUM).cast(FloatType))
      .withColumn(FIELD_TAGS, map(tags: _*))
      .map(r => {
        Chunk.builder()
          .name(r.getAs[String](FIELD_NAME))
          .start(r.getAs[Long](FIELD_START))
          .end(r.getAs[Long](FIELD_END))
          .count(r.getAs[Long](FIELD_COUNT))
          .avg(r.getAs[Double](FIELD_AVG))
          .stdDev(r.getAs[Double](FIELD_STD_DEV))
          .min(r.getAs[Double](FIELD_MIN))
          .max(r.getAs[Double](FIELD_MAX))
          .first(r.getAs[Double](FIELD_FIRST))
          .last(r.getAs[Double](FIELD_LAST))
          .sax(r.getAs[String](FIELD_SAX))
          .qualityMin(r.getAs[Float](FIELD_QUALITY_MIN))
          .qualityMax(r.getAs[Float](FIELD_QUALITY_MAX))
          .qualityFirst(r.getAs[Float](FIELD_QUALITY_FIRST))
          .qualitySum(r.getAs[Float](FIELD_QUALITY_SUM))
          .qualityAvg(r.getAs[Float](FIELD_QUALITY_AVG))
          .value(r.getAs[Array[Byte]](FIELD_VALUE))
          .tags(r.getAs[Map[String, String]](FIELD_TAGS).asJava)
          .buildId()
          .computeMetrics()
          .build()
      })
  }
}