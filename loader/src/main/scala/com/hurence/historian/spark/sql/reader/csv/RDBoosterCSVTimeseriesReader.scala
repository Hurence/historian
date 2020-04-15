package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.LoaderOptions
import com.hurence.historian.model.MeasureRecordV0
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class RDBoosterCSVTimeseriesReader extends CSVTimeseriesReader {

  override def config(): Map[String, String] = Map(
    "inferSchema" -> "true",
    "delimiter" -> ";",
    "header" -> "true",
    "dateFormat" -> ""
  )

  override def read(options: LoaderOptions): Dataset[MeasureRecordV0] = {


    val spark = SparkSession.getActiveSession.get
    import spark.implicits._


    val filePath = options.in

    val ds = spark.read
      .format("csv")
      .options(config())
      .load(filePath)
      .withColumn("day", from_unixtime($"timestamp", "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime($"timestamp")))
      .withColumn("timestamp", $"timestamp" * 1000L)
      .withColumn("name", concat($"metric_name", lit("@"), $"metric_id"))
      .select("name", "timestamp", "value", "day", "hour", "warn", "crit", "min", "max")

      // val filteredDS = if (filterQuery.isDefined) ds.filter(filterQuery.get) else ds
      .repartition($"day")
      .sortWithinPartitions(asc("name"), asc("timestamp"))
    ds.as[MeasureRecordV0]


  }

}
