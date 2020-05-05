package com.hurence.historian.spark.ml


import com.hurence.historian.spark.sql.functions.{chunk, sax}
import org.apache.spark.ml.Model
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{avg, collect_list, count, first, last, lit, max, min, stddev}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}


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

  def this() = this(Identifiable.randomUID("chunkyfier"))

  /** @group setParam */
  final val valueCol: Param[String] = new Param[String](this, "valueCol", "column name for value")

  /** @group setParam */
  def setValueCol(value: String): this.type = set(valueCol, value)
  setDefault(valueCol, "value")


  /** @group setParam */
  final val timestampCol: Param[String] = new Param[String](this, "timestampCol", "column name for timestamp")

  /** @group setParam */
  def setTimestampCol(value: String): this.type = set(timestampCol, value)
  setDefault(timestampCol, "timestamp")


  /** @group setParam */
  final val chunkCol: Param[String] = new Param[String](this, "chunkCol", "column name for encoded chunk")

  /** @group setParam */
  def setChunkCol(value: String): this.type = set(chunkCol, value)
  setDefault(chunkCol, "chunk")

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
    "the chunk max points count",
    ParamValidators.inRange(0, 100000))

  /** @group setParam */
  def setChunkMaxSize(value: Int): this.type = set(chunkMaxSize, value)
  setDefault(chunkMaxSize, 1440)


  def transform(df: Dataset[_]): DataFrame = {

    val grougingCols = col("day") :: $(groupByCols).map(col).toList // "day", "name", "tags.metric_id"
    val w = Window.partitionBy(grougingCols: _*)
      .orderBy(col($(timestampCol)))

    val groupedDF = df
      .withColumn("day", from_unixtime(col($(timestampCol)) / 1000.0, $(dateBucketFormat)))
      .withColumn("values", collect_list(col($(valueCol))).over(w))
      .withColumn("timestamps", collect_list(col($(timestampCol))).over(w))
      .groupBy(grougingCols: _*)
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
        stddev(col($(valueCol))).as("stddev"),
        avg(col($(valueCol))).as("avg"))


    /*  val groupedDF = df
        .groupBy(grougingCols: _*)
        .agg(
          min(df.col("timestamp")).as("start"),
          max(df.col("timestamp")).as("end"),
          collect_list(df.col("value")).as("values"),
          collect_list(df.col("timestamp")).as("timestamps"),
          count(df.col("value")).as("count"),
          min(df.col("value")).as("min"),
          max(df.col("value")).as("max"),
          first(df.col("value")).as("first"),
          last(df.col("value")).as("last"),
          stddev(df.col("value")).as("stddev"),
          avg(df.col("value")).as("avg"),
          first(df.col("tags")).as("tags"))
 */


    // .map(r => (r._1, r._2.sortWith((l1, l2) => l1._1 < l2._1).grouped(1440).toList))

    val chunkDF = groupedDF
      .withColumn($(chunkCol), chunk(
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

    if ($(dropLists)) {
      chunkDF.drop("values", "timestamps")
    } else {
      chunkDF
    }


  }


  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex(chunkCol.name)
    val field = schema.fields(idx)
    if (field.dataType != ArrayType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type ArrayType")
    }
    // Add the return field
    schema.add(StructField(chunkCol.name, StringType, true))
  }

  override def copy(extra: ParamMap): Chunkyfier = {
    defaultCopy[Chunkyfier](extra).setParent(parent)
  }

  override def toString: String = {
    s"Chunkyfier: uid=$uid"
  }
}

