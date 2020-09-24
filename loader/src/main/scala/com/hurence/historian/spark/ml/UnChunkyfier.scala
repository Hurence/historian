package com.hurence.historian.spark.ml

import com.hurence.historian.spark.common.Definitions._
import com.hurence.historian.spark.sql.functions.{sax, unchunk}
import org.apache.spark.ml.Model
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, collect_list, count, first, last, lit, max, min, stddev, _}
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
  * `UnChunkyfier`
  */
final class UnChunkyfier(override val uid: String)
  extends Model[UnChunkyfier] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("unchunkyfier"))


  /** @group setParam */
  final val valueCol: Param[String] = new Param[String](this, "valueCol", "column name for value")

  /** @group setParam */
  def setValueCol(value: String): this.type = set(valueCol, value)
  setDefault(valueCol, "value")


  /** @group setParam */
  final val qualityCol: Param[String] = new Param[String](this, "qualityCol", "column name for quality")

  /** @group setParam */
  def setQualityCol(value: String): this.type = set(qualityCol, value)
  setDefault(qualityCol, "quality")

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


  def transform(df: Dataset[_]): DataFrame = {


   df.withColumn("measures", unchunk(col($(valueCol)), col("start"), col("end")))
      .withColumn("point", explode(col("measures")))
      .select(
        col("name"),
        col("point._2").as($(valueCol) ),
        col("point._1").as($(timestampCol)),
        col("point._3").as($(qualityCol)),
        col("point._4").as("day"),
        col("tags"))
      .drop( "sax", $(valueCol), "avg", "std_dev", "first", "last","min", "max", "count","measures", "start", "end", "point")
  }


  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(valueCol))
    val field = schema.fields(idx)
    if (field.dataType != ArrayType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type ArrayType")
    }
    // Add the return field
    schema.add(StructField($(valueCol), StringType, true))
  }

  override def copy(extra: ParamMap): UnChunkyfier = {
    defaultCopy[UnChunkyfier](extra).setParent(parent)
  }

  override def toString: String = {
    s"UnChunkyfier: uid=$uid"
  }
}

