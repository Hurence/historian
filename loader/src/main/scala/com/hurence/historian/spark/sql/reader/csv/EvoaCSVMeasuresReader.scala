package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.modele.MeasureRecordV0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class EvoaCSVMeasuresReader extends Reader[MeasureRecordV0] {

  def config(): Map[String, String] = Map(
    "inferSchema" -> "true",
    "sep" -> ";",
    "header" -> "true",
    "dateFormat" -> ""
  )


  override def read(options: Options): Dataset[MeasureRecordV0] = {


    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    // Define formats
    val csvRegexp = "((\\w+)\\.?(\\w+-?\\w+-?\\w+)?\\.?(\\w+)?)"
    val dateFmt = "dd/MM/yyyy HH:mm:ss"

    // Load raw data
    val measuresDF = spark.read
      .format("csv")
      .options(options.config)
      .load(options.path)
      .withColumn("time_ms", unix_timestamp($"timestamp", dateFmt) * 1000)
      .withColumn("year", year(to_date($"timestamp", dateFmt)))
      .withColumn("month", month(to_date($"timestamp", dateFmt)))
      .withColumn("day", dayofmonth(to_date($"timestamp", dateFmt)))
      .withColumn("week", weekofyear(to_date($"timestamp", dateFmt)))
      .withColumn("name", regexp_extract($"tagname", csvRegexp, 1))
      .withColumn("code_install", regexp_extract($"tagname", csvRegexp, 2))
      .withColumn("sensor", regexp_extract($"tagname", csvRegexp, 3))
      .withColumn("numeric_type", regexp_extract($"tagname", csvRegexp, 4))
      .select("name", "value", "quality", "code_install", "sensor", "timestamp", "time_ms", "year", "month", "week", "day")
      .orderBy(asc("name"), asc("time_ms"))

    measuresDF.as[MeasureRecordV0]
  }
}
