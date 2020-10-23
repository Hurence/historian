package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.functions.{toDateUTC, toTimestampUTC}
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class GenericMeasuresReaderV0 extends Reader[Measure] {

  private val logger = LoggerFactory.getLogger(classOf[GenericMeasuresReaderV0])

  def config(): Map[String, String] = Map(
    "inferSchema" -> "true",
    "delimiter" -> ",",
    "header" -> "true",
    "nameField" -> "name",
    "timestampField" -> "timestamp",
    "timestampDateFormat" -> "yyyy-MM-dd", // can be set to "s" or "ms" to specifiy a unix timestamp (in sec or ms) instead of a date
    "valueField" -> "value",
    "tagsFields" -> "tag_a,tag_b"
  )

  override def read(options: Options): Dataset[Measure] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    implicit val measureEncoder = Encoders.bean(classOf[Measure])


    val valueField = options.config("valueField")
    val nameField = options.config("nameField")
    val timestampField = options.config("timestampField")
    val timestampDateFormat = options.config("timestampDateFormat")
    val hasQuality = options.config.isDefinedAt("qualityField") && !options.config("qualityField").isEmpty

    val isTimestampInSeconds = timestampDateFormat.equalsIgnoreCase("s") ||
      timestampDateFormat.equalsIgnoreCase("seconds")
    val isTimestampInMilliSeconds = timestampDateFormat.equalsIgnoreCase("ms")

    val tagsFields = options.config("tagsFields")
      .split(",").toList
    val tagsMapping = tagsFields.flatMap(tag => List(lit(tag), col(tag)))


    val mainCols = if (!hasQuality)
      List(
        col(nameField).as("name"),
        col(valueField).as("value"),
        col(timestampField).as("timestamp")) ::: tagsFields.map(tag => col(tag))
    else
      List(
        col(nameField).as("name"),
        col(valueField).as("value"),
        col(options.config("qualityField")).as("quality"),
        col(timestampField).as("timestamp")) ::: tagsFields.map(tag => col(tag))


    val df = spark.read
      .format("csv")
      .options(options.config)
      .load(options.path)
      .select(mainCols: _*)
   //   .withColumn("timestamp", $"timestamp" * 1L)
      .withColumn("tags", map(tagsMapping: _*))

    //
    val dfPlusTime = if (isTimestampInSeconds) {
      logger.info("getting date from timestamp in seconds")
      df.withColumn("timestamp", $"timestamp" * 1000L)
    }
    else if (isTimestampInMilliSeconds) {
      logger.info("getting date from timestamp in milliseconds")
      df.withColumn("timestamp", $"timestamp" * 1L)
    }
    else {
      logger.info(s"getting date from date string with format $timestampDateFormat")
      df.withColumn("timestamp", toTimestampUTC(col("timestamp"), lit(timestampDateFormat)))
    }



   dfPlusTime
   /*   .withColumn("year", year(from_unixtime($"timestamp" / 1000L)))
      .withColumn("month", month(from_unixtime($"timestamp" / 1000L)))
      .withColumn("hour", hour(from_unixtime($"timestamp" / 1000L)))
     // .withColumn("day", from_unixtime($"timestamp" / 1000L, "yyyy-MM-dd"))
      .withColumn("day", toDateUTC($"timestamp", lit("yyyy-MM-dd")))*/
      .drop(tagsFields: _*)
      .map(r => {
        val builder = Measure.builder()

        builder
          .name(r.getAs[String]("name"))
          .timestamp(r.getAs[Long]("timestamp"))
          .value(r.getAs[Double]("value"))
          .tags(r.getAs[Map[String, String]]("tags").asJava)

        if (hasQuality)
          builder.quality(r.getAs[Double]("quality").toFloat)
        else
          builder.quality(java.lang.Float.NaN)

        builder.build()

      })

      .as[Measure]
  }

}
