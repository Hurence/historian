package com.hurence.historian.spark.sql.reader.solr

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._
import org.apache.spark.sql.functions.{col, lit, map, unbase64}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

import scala.collection.JavaConverters._

class SolrChunksReader extends Reader[Chunk] {

  override def read(options: Options): Dataset[_ <: Chunk] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get


    val tagNames: List[Column] = options.config(Options.TAG_NAMES)
      .split(",").toList
      .map(tag => col(tag))
    val mainCols = SOLR_COLUMNS.asScala.toList
      .map(name => col(name).as(getFieldFromColumn(name))) ::: tagNames


    val tags: List[Column] = options.config("tag_names")
      .split(",").toList
      .flatMap(tag => List(lit(tag), col(tag)))

    var df = spark.read
      .format("solr")
      .options(options.config)
      .load()
      .select(mainCols: _*)
      .withColumn(FIELD_VALUE, unbase64(col(FIELD_VALUE)))
      .withColumn(FIELD_TAGS, map(tags: _*))

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
    //
    // This is due to the bean encoder trying to find setters for quality fields with
    // doubles as parameters but only founding float ones (or longs for ints).
    // Thus, we workaround this problem by explicitly casting into floats or ints
    // some columns.
    df = df.withColumn(FIELD_QUALITY_MIN, df(FIELD_QUALITY_MIN).cast(FloatType))
      .withColumn(FIELD_QUALITY_MAX, df(FIELD_QUALITY_MAX).cast(FloatType))
      .withColumn(FIELD_QUALITY_FIRST, df(FIELD_QUALITY_FIRST).cast(FloatType))
      .withColumn(FIELD_QUALITY_SUM, df(FIELD_QUALITY_SUM).cast(FloatType))
      .withColumn(FIELD_QUALITY_AVG, df(FIELD_QUALITY_AVG).cast(FloatType))
      .withColumn(FIELD_YEAR, df(FIELD_YEAR).cast(IntegerType))
      .withColumn(FIELD_MONTH, df(FIELD_MONTH).cast(IntegerType))

    df.as[Chunk](Encoders.bean(classOf[Chunk]))
  }
}
