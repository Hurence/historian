package com.hurence.historian.spark.sql.writer.solr

import com.hurence.historian.LoaderOptions
import com.hurence.historian.spark.sql.writer.TimeseriesWriter
import com.hurence.logisland.record.TimeseriesRecord
import org.apache.spark.sql.Dataset

class SolrTimeseriesWriter extends TimeseriesWriter {


  override def write(options: LoaderOptions, ds: Dataset[TimeseriesRecord]): Unit = {

  }

}
