package com.hurence.historian.spark.sql.reader.csv

import com.hurence.historian.model.HistorianChunkCollectionFieldsVersion0
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.reader.Reader
import com.hurence.timeseries.model.Measure
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class EvoaCSVMeasuresReader extends Reader[Measure] {

  def config(): Map[String, String] = Map(
    "inferSchema" -> "true",
    "sep" -> ";",
    "header" -> "true",
    "dateFormat" -> ""
  )


  override def read(options: Options): Dataset[Measure] = {


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
      .withColumn(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR, year(to_date($"timestamp", dateFmt)))
      .withColumn(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH, month(to_date($"timestamp", dateFmt)))
      .withColumn(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY, dayofmonth(to_date($"timestamp", dateFmt)))
      .withColumn(HistorianChunkCollectionFieldsVersion0.NAME, regexp_extract($"tagname", csvRegexp, 1))
      //TODO should add those as tags
      .withColumn("code_install", regexp_extract($"tagname", csvRegexp, 2))
      .withColumn("sensor", regexp_extract($"tagname", csvRegexp, 3))
      .select("name", "value", "quality", "code_install",
        "sensor", "timestamp", "time_ms", "year", "month", "week", "day")
      .orderBy(asc("name"), asc("timestamp"))

//    measuresDF.as[MeasureVersionV0]
    null
    //TODO
  }
}
