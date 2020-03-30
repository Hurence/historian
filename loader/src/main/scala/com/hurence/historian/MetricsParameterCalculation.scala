
package com.hurence.historian
import com.hurence.logisland.timeseries.sax.SaxParametersGuess
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => f}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.{WrappedArray => ArrayDF}

object MetricsParameterCalculation {

  def main(args: Array[String]): Unit = {
    val path = "C:/Users/pc-asus/Work/Github/historian/loader/src/main/resources/part-00186-2955ddb8-10c7-48cc-b635-4eb7240b3e7e.c000.snappy.parquet"
    print(ParamCalculation(path))

  }

  private def ParamCalculation(path: String): List[Double] = {
    val spark = SparkSession.builder
      .appName("DataLoader")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //Limit the number of messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val timeStart = System.currentTimeMillis()

    val df = spark.read.parquet(path)
      .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .withColumn("name", concat($"metric_name", lit("@"), $"metric_id"))
      .filter("metric_name = 'ack'")
      .map(r => (
        (r.getAs[String]("day"), r.getAs[String]("name")),
        List((r.getAs[Long]("timestamp"), r.getAs[Double]("value")))))
      .rdd
      .reduceByKey((g1, g2) => g1 ::: g2)
      .map(r => (r._1, r._2.sortWith((l1, l2) => l1._1 < l2._1).grouped(1440).toList))
      .toDF()
    df.show(5,50)
    df.printSchema()

    val values = df.map(r => r.getAs[ArrayDF[Double]]("_2"))


    val numberOfRows = df.count()
    println("Number of rows : ", numberOfRows)



//    val res = df.groupBy($"name").agg(count($"name").as("count"), min($"value")
//      .as("min"), max($"value").as("max"), sum("value").as("sum")).show()


    //    val getParam = SaxParametersGuess.guess(vcaluesres.slice(0,1500)
    //                                            ,10,200,10
    //                                            ,2,10,1
    //                                            ,2,10,1).asScala.toList


    //    val duration = (System.currentTimeMillis() - timeStart)
    //    println("Execution duration :", duration/1000.0)
    //
    //    finalResult = finalResult ::: getParam.asInstanceOf[List[Double]]
    //    println(getParam)
    //    println("***********************************************************************************")
    var finalResult = List[Double]()
    finalResult
  }
}