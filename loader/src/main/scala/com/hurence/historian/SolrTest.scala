package com.hurence.historian

import com.hurence.logisland.record.TimeSeriesRecord
import org.apache.spark.sql.SparkSession


object SolrTest {


  def main(args: Array[String]): Unit = {


    // setup spark session
    val spark = SparkSession.builder
      .appName("test")
      .master("local[*]")
      .getOrCreate()


    val solrOpts = Map(
      "zkhost" -> "zookeeper:2181",
      "collection" -> "historian",
      "fields" -> "id,name,chunk_value,chunk_start,chunk_end",
      "filters" -> s"chunk_origin:logisland AND year:2019 AND month:6 AND day:20"
    )

    val df = spark.read.format("solr")
      .options(solrOpts)
      .load


    df.show()



/*
    import spark.implicits._

    val ds = df.map(r => new TimeSeriesRecord("evoa_measure",
      r.getAs[String]("id"),
      r.getAs[String]("name"),
      r.getAs[String]("chunk_value"),
      r.getAs[Long]("chunk_start"),
      r.getAs[Long]("chunk_end")))

    mergeChunks(ds,options).take(20).foreach(r=> println(s"${r.getMetricName} : ${r.getTimeSeries.size()} points, chunk_value:${r.getField(TimeSeriesRecord.CHUNK_VALUE).asDouble() }"))


    mergedTimeseriesDS
      .groupByKey(_.getMetricName)
      .reduceGroups((g1, g2) => merge(g1, g2))
      .map(r => r._2)

*/

    spark.close()

  }
}
