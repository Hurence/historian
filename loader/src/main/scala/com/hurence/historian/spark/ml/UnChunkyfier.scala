package com.hurence.historian.spark.ml

import com.hurence.historian.spark.sql.functions.{sax, unchunk}
import com.hurence.timeseries.model.{Chunk, Measure}
import com.hurence.timeseries.model.Definitions._
import org.apache.spark.ml.Model
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, collect_list, count, first, last, lit, max, min, stddev, _}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.collection.JavaConverters._


/**
  * UnChunkyfier is a transformer that takes a Dataframe with at least a name/value/start/end/tags columns
  * and makes Measures from that chunk. all others columns should be dropped
  */
final class UnChunkyfier(override val uid: String)
  extends Model[UnChunkyfier] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("unchunkyfier"))

  ////////////////////////////////////////////
  // Parameters
  ////////////////////////////////////////////

  val valueCol: Param[String] = new Param[String](this, "valueCol", "input column name for value")
  def setValueCol(value: String): this.type = set(valueCol, value)
  setDefault(valueCol, FIELD_VALUE)

  val nameCol: Param[String] = new Param[String](this, "nameCol", "input column name for name")
  def setNameCol(value: String): this.type = set(nameCol, value)
  setDefault(nameCol, FIELD_NAME)

  val startCol: Param[String] = new Param[String](this, "startCol", "input column name for start")
  def setStartCol(value: String): this.type = set(startCol, value)
  setDefault(startCol, FIELD_START)

  val endCol: Param[String] = new Param[String](this, "endCol", "input column name for end")
  def setEndCol(value: String): this.type = set(endCol, value)
  setDefault(endCol, FIELD_END)

  val tagsCol: Param[String] = new Param[String](this, "tagsCol", "input column name for tags")
  def setTagsCol(value: String): this.type = set(tagsCol, value)
  setDefault(tagsCol, FIELD_TAGS)

  val dateBucketFormat: Param[String] = new Param[String](this, "dateBucketFormat", "date bucket format as java string date")
  def setDateBucketFormat(value: String): this.type = set(dateBucketFormat, value)
  setDefault(dateBucketFormat, "yyyy-MM-dd")


  ////////////////////////////////////////////
  // Business
  ////////////////////////////////////////////

  def transform(df: Dataset[_]): DataFrame = {
    implicit val encoder = Encoders.bean(classOf[Measure])

    df.withColumn("measures",
      unchunk(
        col($(valueCol)),
        col($(startCol)),
        col($(endCol))))
      .withColumn("measure", explode(col("measures")))
      .select(
        col($(nameCol)),
        col("measure._2").as(FIELD_VALUE),
        col("measure._1").as(FIELD_TIMESTAMP),
        col("measure._3").as(FIELD_QUALITY),
        col("measure._4").as(FIELD_DAY),
        col($(tagsCol)))
      .map(r => {
        Measure.builder()
          .name(r.getAs[String](FIELD_NAME))
          .timestamp(r.getAs[Long](FIELD_TIMESTAMP))
          .value(r.getAs[Double](FIELD_VALUE))
          .tags(r.getAs[Map[String, String]](FIELD_TAGS).asJava)
          .quality(r.getAs[Float](FIELD_QUALITY))
          .build()
      }).toDF()
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

