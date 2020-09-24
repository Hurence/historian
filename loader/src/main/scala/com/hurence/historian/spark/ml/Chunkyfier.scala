package com.hurence.historian.spark.ml


import com.hurence.historian.spark.common.Definitions._
import com.hurence.historian.spark.sql.functions.{chunk, sax, toDateUTC}
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.core.ChunkOrigin
import com.hurence.timeseries.model.Chunk
import org.apache.spark.ml.Model
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{avg, collect_list, count, first, last, lit, max, min, stddev}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
  * `Chunkytizer` maps a column of continuous features to a column of feature buckets.
  *
  * Since 2.3.0,
  * `Bucketizer` can map multiple columns at once by setting the `inputCols` parameter. Note that
  * when both the `inputCol` and `inputCols` parameters are set, an Exception will be thrown. The
  * `splits` parameter is only used for single column usage, and `splitsArray` is for multiple
  * columns.
  */
final class Chunkyfier(override val uid: String)
  extends Model[Chunkyfier] with DefaultParamsWritable {

  var withQuality: Boolean = false

  def this() = this(Identifiable.randomUID("chunkyfier"))

  /** @group setParam */
  final val valueCol: Param[String] = new Param[String](this, "valueCol", "column name for value")

  /** @group setParam */
  def setValueCol(value: String): this.type = set(valueCol, value)
  setDefault(valueCol, "value")

  /** @group setParam */
  final val qualityCol: Param[String] = new Param[String](this, "qualityCol", "column name for quality")

  /** @group setParam */
  def setQualityCol(value: String): this.type = {
    withQuality = true
    set(qualityCol, value)
  }
  setDefault(qualityCol, "quality")

  /** @group setParam */
  final val origin: Param[String] = new Param[String](this, "origin", "which historian component wrote this chunk")

  /** @group setParam */
  def setOrigin(value: String): this.type = set(origin, value)
  setDefault(origin, ChunkOrigin.INJECTOR.toString)

  /** @group setParam */
  final val timestampCol: Param[String] = new Param[String](this, "timestampCol", "column name for timestamp")

  /** @group setParam */
  def setTimestampCol(value: String): this.type = set(timestampCol, value)
  setDefault(timestampCol, "timestamp")

  /** @group setParam */
  final val dropLists: Param[Boolean] = new Param[Boolean](this, "dropLists", "do we drop the values and timestamps columns")

  /** @group setParam */
  def doDropLists(value: Boolean): this.type = set(dropLists, value)
  setDefault(dropLists, true)

  /** @group param */
  final val dateBucketFormat: Param[String] = new Param[String](this, "dateBucketFormat", "date bucket format as java string date")

  /** @group setParam */
  def setDateBucketFormat(value: String): this.type = set(dateBucketFormat, value)
  setDefault(dateBucketFormat, "yyyy-MM-dd")

  /**
    * Param for group by column names.
    *
    * @group param
    */
  final val groupByCols: StringArrayParam = new StringArrayParam(this, "groupByCols", "group by column names")

  /** @group setParam */
  def setGroupByCols(value: Array[String]): this.type = set(groupByCols, value)


  val saxAlphabetSize: Param[Int] = new Param[Int](this, "saxAlphabetSize",
    "the SAX akphabet size.",
    ParamValidators.inRange(0, 20))

  /** @group setParam */
  def setSaxAlphabetSize(value: Int): this.type = set(saxAlphabetSize, value)
  setDefault(saxAlphabetSize, 5)

  val saxStringLength: Param[Int] = new Param[Int](this, "saxStringLength",
    "the SAX string length",
    ParamValidators.inRange(0, 10000))

  /** @group setParam */
  def setSaxStringLength(value: Int): this.type = set(saxStringLength, value)
  setDefault(saxStringLength, 20)

  val chunkMaxSize: Param[Int] = new Param[Int](this, "chunkMaxSize",
    "the chunk max measures count",
    ParamValidators.inRange(0, 100000))

  /** @group setParam */
  def setChunkMaxSize(value: Int): this.type = set(chunkMaxSize, value)
  setDefault(chunkMaxSize, 1440)


  /** @group setParam */
  final val chunkValueCol: Param[String] = new Param[String](this, "chunkValueCol", "column name for chunk Value")

  /** @group setParam */
  def setChunkValueCol(value: String): this.type = set(chunkValueCol, value)
  setDefault(chunkValueCol, "value")

  def transform(df: Dataset[_]): DataFrame = {
    implicit val chunkEncoder = Encoders.bean(classOf[Chunk])

    val groupingCols = col("day") :: $(groupByCols).map(col).toList // "day", "name", "tags.metric_id"
    val w = Window.partitionBy(groupingCols: _*)
      .orderBy(col($(timestampCol)))

    var baseDf = df
    // If no quality column has in the input df, create one default with 0 as value
    if (!withQuality) {
      baseDf = baseDf.withColumn("quality", lit(0f))
    }

    val groupedDF = baseDf
      .withColumn("day", toDateUTC(col($(timestampCol)), lit($(dateBucketFormat))))
      .withColumn("values", collect_list(col($(valueCol))).over(w))
      .withColumn("timestamps", collect_list(col($(timestampCol))).over(w))
      .groupBy(groupingCols: _*)
      .agg(
        last(col("values")).as("values"),
        last(col("timestamps")).as("timestamps"),
        first(col("tags")).as("tags"),
        min(col($(timestampCol))).as("start"),
        max(col($(timestampCol))).as("end"),
        count(col($(valueCol))).as("count"),
        min(col($(valueCol))).as("min"),
        max(col($(valueCol))).as("max"),
        first(col($(valueCol))).as("first"),
        last(col($(valueCol))).as("last"),
        stddev(col($(valueCol))).as("std_dev"),
        avg(col($(valueCol))).as("avg"),
        min(col($(qualityCol))).as("quality_min"),
        max(col($(qualityCol))).as("quality_max"),
        first(col($(qualityCol))).as("quality_first"),
        sum(col($(qualityCol))).as("quality_sum"),
        avg(col($(qualityCol))).as("quality_avg"))


    groupedDF
      .withColumn($(chunkValueCol), chunk(
        groupedDF.col("name"),
        groupedDF.col("start"),
        groupedDF.col("end"),
        groupedDF.col("timestamps"),
        groupedDF.col("values")))
      .withColumn("sax", sax(
        lit($(saxAlphabetSize)),
        lit(0.01),
        lit($(saxStringLength)),
        groupedDF.col("values")))

      .drop("values", "timestamps")
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
          .origin($(origin))
          .first(r.getAs[Double]("first"))
          .last(r.getAs[Double]("last"))
          .sax(r.getAs[String]("sax"))
          .value(r.getAs[Array[Byte]]($(chunkValueCol)))
          .tags(r.getAs[Map[String, String]]("tags").asJava)
          .qualityMin(r.getAs[Float]("quality_min"))
          .qualityMax(r.getAs[Float]("quality_max"))
          .qualityFirst(r.getAs[Float]("quality_first"))
          .qualitySum(r.getAs[Double]("quality_sum").toFloat)
          .qualityAvg(r.getAs[Double]("quality_avg").toFloat)
          .buildId()
          .computeMetrics()
          .build()
      })
      .as[Chunk]
      .toDF()

  }

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex(CHUNK_COLUMN)
    val field = schema.fields(idx)
    if (field.dataType != ArrayType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type ArrayType")
    }
    // Add the return field
    schema.add(StructField(CHUNK_COLUMN, StringType, true))
  }

  override def copy(extra: ParamMap): Chunkyfier = {
    defaultCopy[Chunkyfier](extra).setParent(parent)
  }

  override def toString: String = {
    s"Chunkyfier: uid=$uid"
  }
}

