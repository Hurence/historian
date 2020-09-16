package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import scala.collection.JavaConverters._

class ITDataCSVMeasuresReaderV0 extends Reader[Measure] {

  def config(): Map[String, String] = Map(
    "inferSchema" -> "true",
    "delimiter" -> ",",
    "header" -> "true",
    "dateFormat" -> ""
  )

  override def read(options: Options): Dataset[Measure] = {


    val spark = SparkSession.getActiveSession.get

    import spark.implicits._
    implicit val measureEncoder = Encoders.bean(classOf[Measure])

    spark.read
      .format("csv")
      .options(options.config)
      .load(options.path)
      .withColumn("year", year(from_unixtime(col("timestamp"))))
      .withColumn("month", month(from_unixtime(col("timestamp"))))
      .withColumn("day", from_unixtime(col("timestamp"), "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime(col("timestamp"))))
      .withColumn("timestamp", col("timestamp") * 1000L)
      .withColumn("name", col("metric_name"))
      .withColumn("tags", map(
        lit("metric_id"), col("metric_id"),
        lit("warn"), col("warn"),
        lit("crit"), col("crit"),
        lit("min"), col("min"),
        lit("max"), col("max")))
      .select("name", "value", "timestamp", "year", "month", "day", "hour", "tags")
      .map(r => Measure.builder()
        .name(r.getAs[String]("name"))
        .timestamp(r.getAs[Long]("timestamp"))
        .value(r.getAs[Double]("value"))
        .tags(r.getAs[Map[String, String]]("tags").asJava)
        .compute()
        .build()
      )
      .as[Measure]
      .repartition($"day")
      .sortWithinPartitions(asc("name"), asc("timestamp"))


  }

}
