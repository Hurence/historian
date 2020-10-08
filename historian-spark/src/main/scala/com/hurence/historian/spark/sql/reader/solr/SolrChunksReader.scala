package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.Options.TAG_NAMES
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Chunk.ChunkBuilder
import com.hurence.timeseries.model.Definitions._
import org.apache.spark.sql.functions.{col, lit, map, unbase64}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class SolrChunksReader extends Reader[Chunk] {

  override def read(options: Options): Dataset[_ <: Chunk] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get

    implicit val encoder = Encoders.bean(classOf[Chunk])

    var someTags : Boolean = true
    val tagNames: List[Column] = if (options.config.contains(TAG_NAMES)) {
      options.config(TAG_NAMES)
        .split(",").toList
        .map(tag => col(tag).as(s"tag_$tag"))
    } else {
      // No tags specified
      someTags = false
      List[Column]()
    }
    val mainCols = SOLR_COLUMNS.asScala.toList
      .map(name => col(name).as(getFieldFromColumn(name))) ::: tagNames

    var tags : List[Column] = List[Column]()

    if (someTags) {
      tags = options.config(TAG_NAMES)
        .split(",").toList
        .flatMap(tag => List(lit(s"$tag"), col(s"tag_$tag")))
    }

    var chunksDf = spark.read
      .format("solr")
      .options(options.config)
      .load()
      .select(mainCols: _*)
      .withColumn(FIELD_VALUE, unbase64(col(FIELD_VALUE)))
      // Cast quality_* fields from double to float due to this bug:
      // https://issues.apache.org/jira/browse/SPARK-12036
      // Somebody having the same problem:
      // https://stackoverflow.com/questions/58445952/spark-fails-to-map-json-integer-to-java-integer
      // We store quality_* fields as float in Solr to gain storage space.
      // But when reading Chunks here, the schema shows they are read back into doubles.
      // Later when we try to create the Chunks from this dataframe (lazy operation),
      // we get an error like this one:
      //
      // failed to compile: org.codehaus.commons.compiler.CompileException: File 'generated.java',
      // Line 152, Column 27: No applicable constructor/method found for actual parameters "double";
      // candidates are: "public void com.hurence.timeseries.model.Chunk.setQuality_max(float)"

      // This is due to the bean encoder trying to find setters for quality fields with
      // doubles as parameters but only founding float ones (or longs for ints).
      // Thus, we workaround this problem by explicitly casting into floats or ints
      // some columns.
      .withColumn(FIELD_QUALITY_AVG, col(FIELD_QUALITY_AVG).cast(FloatType))
      .withColumn(FIELD_QUALITY_FIRST, col(FIELD_QUALITY_FIRST).cast(FloatType))
      .withColumn(FIELD_QUALITY_MIN, col(FIELD_QUALITY_MIN).cast(FloatType))
      .withColumn(FIELD_QUALITY_MAX, col(FIELD_QUALITY_MAX).cast(FloatType))
      .withColumn(FIELD_QUALITY_SUM, col(FIELD_QUALITY_SUM).cast(FloatType))
      .withColumn(FIELD_YEAR, col(FIELD_YEAR).cast(IntegerType))
      .withColumn(FIELD_MONTH, col(FIELD_MONTH).cast(IntegerType))

    if (someTags) {
      chunksDf = chunksDf
        .withColumn(FIELD_TAGS, map(tags: _*))
    }

    chunksDf.map(r => {
        val builder : ChunkBuilder = Chunk.builder()
          .name(r.getAs[String](FIELD_NAME))
          .origin(r.getAs[String](FIELD_ORIGIN))
          .start(r.getAs[Long](FIELD_START))
          .end(r.getAs[Long](FIELD_END))
          .count(r.getAs[Long](FIELD_COUNT))
          .avg(r.getAs[Double](FIELD_AVG))
          .stdDev(r.getAs[Double](FIELD_STD_DEV))
          .min(r.getAs[Double](FIELD_MIN))
          .max(r.getAs[Double](FIELD_MAX))
          .sum(r.getAs[Double](FIELD_SUM))
          .first(r.getAs[Double](FIELD_FIRST))
          .last(r.getAs[Double](FIELD_LAST))
          .sax(r.getAs[String](FIELD_SAX))
          .qualityMin(r.getAs[Float](FIELD_QUALITY_MIN))
          .qualityMax(r.getAs[Float](FIELD_QUALITY_MAX))
          .qualityFirst(r.getAs[Float](FIELD_QUALITY_FIRST))
          .qualitySum(r.getAs[Float](FIELD_QUALITY_SUM))
          .qualityAvg(r.getAs[Float](FIELD_QUALITY_AVG))
          .trend(r.getAs[Boolean](FIELD_TREND))
          .outlier(r.getAs[Boolean](FIELD_OUTLIER))
          .value(r.getAs[Array[Byte]](FIELD_VALUE))

        if (someTags) {
          builder.tags(r.getAs[Map[String, String]](FIELD_TAGS).asJava)
        }

        builder.buildId()
          .computeMetrics()
          .build()
      })
  }
}
