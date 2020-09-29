package com.hurence.historian.spark.ml

import com.hurence.historian.spark.sql.functions.{chunk, sax, toDateUTC}
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.core.ChunkOrigin
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Definitions._
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


  val valueCol: Param[String] = new Param[String](this, "valueCol", "column name for value")
  def setValueCol(value: String): this.type = set(valueCol, value)
  setDefault(valueCol, FIELD_VALUE)

  val qualityCol: Param[String] = new Param[String](this, "qualityCol", "column name for quality")
  def setQualityCol(value: String): this.type = {
    withQuality = true
    set(qualityCol, value)
  }
  setDefault(qualityCol, FIELD_QUALITY)

  val origin: Param[String] = new Param[String](this, "origin", "which historian component wrote this chunk")
  def setOrigin(value: String): this.type = set(origin, value)
  setDefault(origin, ChunkOrigin.INJECTOR.toString)

  val timestampCol: Param[String] = new Param[String](this, "timestampCol", "column name for timestamp")
  def setTimestampCol(value: String): this.type = set(timestampCol, value)
  setDefault(timestampCol, FIELD_TIMESTAMP)

  val dropLists: Param[Boolean] = new Param[Boolean](this, "dropLists", "do we drop the values and timestamps columns")
  def doDropLists(value: Boolean): this.type = set(dropLists, value)
  setDefault(dropLists, true)


  val dateBucketFormat: Param[String] = new Param[String](this, "dateBucketFormat", "date bucket format as java string date")
  def setDateBucketFormat(value: String): this.type = set(dateBucketFormat, value)
  setDefault(dateBucketFormat, "yyyy-MM-dd")

  /**
    * Param for group by column names.
    *
    * @group param
    */
  final val groupByCols: StringArrayParam = new StringArrayParam(this, "groupByCols", "group by column names")
  def setGroupByCols(value: Array[String]): this.type = set(groupByCols, value)


  val saxAlphabetSize: Param[Int] = new Param[Int](this, "saxAlphabetSize",
    "the SAX akphabet size.",
    ParamValidators.inRange(0, 20))
  def setSaxAlphabetSize(value: Int): this.type = set(saxAlphabetSize, value)
  setDefault(saxAlphabetSize, 5)

  val saxStringLength: Param[Int] = new Param[Int](this, "saxStringLength",
    "the SAX string length",
    ParamValidators.inRange(0, 10000))
  def setSaxStringLength(value: Int): this.type = set(saxStringLength, value)
  setDefault(saxStringLength, 20)

  val chunkMaxSize: Param[Int] = new Param[Int](this, "chunkMaxSize",
    "the chunk max measures count",
    ParamValidators.inRange(0, 100000))
  def setChunkMaxSize(value: Int): this.type = set(chunkMaxSize, value)
  setDefault(chunkMaxSize, 1440)


  final val chunkValueCol: Param[String] = new Param[String](this, "chunkValueCol", "column name for chunk Value")
  def setChunkValueCol(value: String): this.type = set(chunkValueCol, value)
  setDefault(chunkValueCol, FIELD_VALUE)

  final val tagsCol: Param[String] = new Param[String](this, "tagsCol", "column name for tags")
  def setTagsCol(value: String): this.type = set(tagsCol, value)
  setDefault(tagsCol, FIELD_TAGS)


  val COLUMN_VALUES = "values"
  val COLUMN_TIMESTAMPS = "timestamps"

  def transform(df: Dataset[_]): DataFrame = {
    implicit val chunkEncoder = Encoders.bean(classOf[Chunk])

    val groupingCols = col(FIELD_DAY) :: $(groupByCols).map(col).toList // "day", "name", "tags.metric_id"
    val w = Window.partitionBy(groupingCols: _*)
      .orderBy(col($(timestampCol)))

    var baseDf = df
    // If no quality column has in the input df, create one default with NaN as value
    if (!withQuality) {
      baseDf = baseDf.withColumn(FIELD_QUALITY, lit(Float.NaN))
    }

    val groupedDF = baseDf
      .withColumn(FIELD_DAY, toDateUTC(col($(timestampCol)), lit($(dateBucketFormat))))
      .withColumn(COLUMN_VALUES, collect_list(col($(valueCol))).over(w))
      .withColumn(COLUMN_TIMESTAMPS, collect_list(col($(timestampCol))).over(w))
      .groupBy(groupingCols: _*)
      // compute all the stats and aggregtions here
      .agg(
        last(col(COLUMN_VALUES)).as(COLUMN_VALUES),
        last(col(COLUMN_TIMESTAMPS)).as(COLUMN_TIMESTAMPS),
        first(col($(tagsCol))).as(FIELD_TAGS),
        min(col($(timestampCol))).as(FIELD_START),
        max(col($(timestampCol))).as(FIELD_END),
        count(col($(valueCol))).as(FIELD_COUNT),
        sum(col($(valueCol))).as(FIELD_SUM),
        min(col($(valueCol))).as(FIELD_MIN),
        max(col($(valueCol))).as(FIELD_MAX),
        first(col($(valueCol))).as(FIELD_FIRST),
        last(col($(valueCol))).as(FIELD_LAST),
        stddev(col($(valueCol))).as(FIELD_STD_DEV),
        avg(col($(valueCol))).as(FIELD_AVG),
        min(col($(qualityCol))).as(FIELD_QUALITY_MIN),
        max(col($(qualityCol))).as(FIELD_QUALITY_MAX),
        first(col($(qualityCol))).as(FIELD_QUALITY_FIRST),
        sum(col($(qualityCol))).as(FIELD_QUALITY_SUM),
        avg(col($(qualityCol))).as(FIELD_QUALITY_AVG))


    groupedDF
      // compute chunk bytes from values & timestamps lists
      .withColumn($(chunkValueCol), chunk(
        groupedDF.col(FIELD_NAME),
        groupedDF.col(FIELD_START),
        groupedDF.col(FIELD_END),
        groupedDF.col(COLUMN_TIMESTAMPS),
        groupedDF.col(COLUMN_VALUES)))
      // compute SAX string for chunk
      .withColumn(FIELD_SAX, sax(
        lit($(saxAlphabetSize)),
        lit(0.01),
        lit($(saxStringLength)),
        groupedDF.col(COLUMN_VALUES)))
      // drop temporary values & timestamps columns
      .drop(COLUMN_VALUES, COLUMN_TIMESTAMPS)
      .map(r => {

        Chunk.builder()
          .name(r.getAs[String](FIELD_NAME))
          .origin($(origin))
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
          .value(r.getAs[Array[Byte]]($(chunkValueCol)))
          .tags(r.getAs[Map[String, String]](FIELD_TAGS).asJava)
          .qualityMin(r.getAs[Float](FIELD_QUALITY_MIN))
          .qualityMax(r.getAs[Float](FIELD_QUALITY_MAX))
          .qualityFirst(r.getAs[Float](FIELD_QUALITY_FIRST))
          .qualitySum(r.getAs[Double](FIELD_QUALITY_SUM).toFloat)
          .qualityAvg(r.getAs[Double](FIELD_QUALITY_AVG).toFloat)
          .buildId()
          .computeMetrics()
          .build()
      })
      .toDF()

  }

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex(FIELD_VALUE)
    val field = schema.fields(idx)
    if (field.dataType != ArrayType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type ArrayType")
    }
    // Add the return field
    schema.add(StructField(FIELD_VALUE, StringType, true))
  }

  override def copy(extra: ParamMap): Chunkyfier = {
    defaultCopy[Chunkyfier](extra).setParent(parent)
  }

  override def toString: String = {
    s"Chunkyfier: uid=$uid"
  }
}

