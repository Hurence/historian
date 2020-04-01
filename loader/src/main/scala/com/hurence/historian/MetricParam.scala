package com.hurence.historian
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{asc, concat, from_unixtime, hour, lit}
import org.apache.spark.sql.{functions => f}
object MetricParam {
  def main(args: Array[String]): Unit = {
    val path = "C:/Users/pc-asus/Work/Github/historian/loader/src/main/resources/part-00186-2955ddb8-10c7-48cc-b635-4eb7240b3e7e.c000.snappy.parquet"
    print(ParamCalculation(path))

  }
  private def ParamCalculation(path: String): List[Any] = {
    val spark = SparkSession
      .builder()
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(path)
      .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .map(r => (
        (r.getAs[String]("day"), r.getAs[String]("metric_id"), r.getAs[String]("metric_name")),
        List((r.getAs[Long]("timestamp"), r.getAs[Double]("value")))))
      .rdd
      .reduceByKey((g1, g2) => g1 ::: g2)
      .map(r => (r._1, r._2.sortWith((l1, l2) => l1._1 < l2._1).grouped(1440).toList))
      .cache().toDF()

    df.show()
    df.printSchema()

    val result  = List[Any]()
    result
  }



}
