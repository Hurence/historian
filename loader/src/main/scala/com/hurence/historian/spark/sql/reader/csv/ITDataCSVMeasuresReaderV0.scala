package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.model.{ MeasureRecordV0}
import com.hurence.historian.spark.sql.reader.{Reader, ReaderOptions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ lit}
import org.apache.spark.sql.{Dataset, SparkSession}

class ITDataCSVMeasuresReaderV0 extends Reader[MeasureRecordV0] {

  def config(): Map[String, String] = Map(
    "inferSchema" -> "true",
    "delimiter" -> ",",
    "header" -> "true",
    "dateFormat" -> ""
  )

  override def read(options: ReaderOptions): Dataset[MeasureRecordV0] = {


    val spark = SparkSession.getActiveSession.get
    import spark.implicits._


    val filePath = options.in


    spark.read
      .format("csv")
      .options(config())
      .load(filePath)
      .withColumn("year", year(from_unixtime($"timestamp")))
      .withColumn("month", month(from_unixtime($"timestamp")))
      .withColumn("day", from_unixtime($"timestamp", "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime($"timestamp")))
      .withColumn("timestamp", $"timestamp" * 1000L)
      .withColumn("name", $"metric_name")
      .withColumn("tags", map(
        lit("metric_id"), $"metric_id",
        lit("warn"), $"warn",
        lit("crit"), $"crit",
        lit("min"), $"min",
        lit("max"), $"max"))
      .select("name", "value", "timestamp", "year", "month", "day", "hour", "tags")
      .as[MeasureRecordV0]
    //  .filter("day LIKE '2019-11-2%' OR day LIKE '2019-11-3%'")
  //    .filter("name = 'ack' OR name LIKE 'consumers%' OR name LIKE 'messages%' OR name LIKE 'memory%' OR name LIKE 'cpu%'")
      .repartition($"day")
      .sortWithinPartitions(asc("name"), asc("timestamp"))


  }

}
