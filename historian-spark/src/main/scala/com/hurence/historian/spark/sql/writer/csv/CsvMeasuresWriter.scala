package com.hurence.historian.spark.sql.writer.csv

import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.Options.TAG_NAMES
import com.hurence.historian.spark.sql.writer.Writer
import com.hurence.timeseries.model.Definitions.{FIELDS, FIELD_NAME, FIELD_QUALITY, FIELD_TAGS, FIELD_TIMESTAMP, FIELD_VALUE, getColumnFromField}
import com.hurence.timeseries.model.{Chunk, Measure}
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.col



class CsvMeasuresWriter extends Writer[Measure] {

  override def write(options: Options, ds: Dataset[_ <: Measure]) = {

    var someTags : Boolean = true
    val tagCols : List[Column] = if (options.config.contains(TAG_NAMES)) {
      options.config(TAG_NAMES).split(",").toList
        .map(tag => col(FIELD_TAGS)(tag).as(tag))
    } else  {
      // No tags specified
      someTags = false
      List[Column]()
    }

    val mainCols = List( col(FIELD_NAME), col(FIELD_QUALITY), col(FIELD_VALUE), col(FIELD_TIMESTAMP))

    ds
      .select(mainCols ::: tagCols: _*)
      .write
      .mode("overwrite")
      .options(options.config)
      .csv(options.path)
  }

}
