package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.model.{ChunkRecordV0, MeasureRecordV0}
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.functions.{toDateUTC, toTimestampUTC}
import com.hurence.historian.spark.sql.reader.Reader
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

class GenericMeasuresReaderV0 extends Reader[MeasureRecordV0] {

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

  override def read(options: Options): Dataset[MeasureRecordV0] = {
    // 5. load back those chunks to verify
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._


    val valueField = options.config("valueField")
    val nameField = options.config("nameField")
    val timestampField = options.config("timestampField")
    val timestampDateFormat = options.config("timestampDateFormat")

    val isTimestampInSeconds = timestampDateFormat.equalsIgnoreCase("s") ||
      timestampDateFormat.equalsIgnoreCase("seconds")
    val isTimestampInMilliSeconds = timestampDateFormat.equalsIgnoreCase("ms")

    val tagsFields = options.config("tagsFields")
      .split(",").toList
    val tagsMapping = tagsFields.flatMap(tag => List(lit(tag), col(tag)))

    val mainCols = List(
      col(nameField).as("name"),
      col(valueField).as("value"),
      col(timestampField).as("timestamp")) ::: tagsFields.map(tag => col(tag))


    val df = spark.read
      .format("csv")
      .options(options.config)
      .load(options.path)
      .select(mainCols: _*)
      .withColumn("timestamp", $"timestamp" * 1L)
      .withColumn("tags", map(tagsMapping: _*))

    //
    val dfPlusTime = if (isTimestampInSeconds) {
      logger.info("getting date from timestamp in seconds")
      df.withColumn("timestamp", $"timestamp" * 1000L)
    }
    else if(isTimestampInMilliSeconds)  {
      logger.info("getting date from timestamp in milliseconds")
      df.withColumn("timestamp", $"timestamp" * 1L)
    }
    else {
      logger.info(s"getting date from date string with format $timestampDateFormat")
      df.withColumn("timestamp", toTimestampUTC(col("timestamp"), lit(timestampDateFormat)))
    }


    dfPlusTime
      .withColumn("year", year(from_unixtime($"timestamp" / 1000L)))
      .withColumn("month", month(from_unixtime($"timestamp" / 1000L)))
      .withColumn("hour", hour(from_unixtime($"timestamp" / 1000L)))
      .withColumn("day", from_unixtime($"timestamp"/ 1000L, "yyyy-MM-dd"))
     // .withColumn("day", toDateUTC($"timestamp", lit("yyyy-MM-dd")))
      .drop(tagsFields: _*)
      .as[MeasureRecordV0]

  }

}
