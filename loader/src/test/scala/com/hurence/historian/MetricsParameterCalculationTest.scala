package com.hurence.historian

import com.hurence.unit5.extensions.SparkExtension
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, from_unixtime, lit}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

import scala.collection.mutable.{WrappedArray => ArrayDF}

@ExtendWith(Array(classOf[SparkExtension]))
class MetricsParameterCalculationTest {

  @Test
  def main(sparkSession: SparkSession): Unit = {
    print(ParamCalculation(sparkSession))
  }

  private def ParamCalculation(sparkSession: SparkSession): List[Double] = {
//    val path = getClass.getResource("/parquet/part-00186-2955ddb8-10c7-48cc-b635-4eb7240b3e7e.c000.snappy.parquet").getFile
    val path = getClass.getResource("/parquet/it-data.parquet").getFile
    import sparkSession.implicits._

    //Limit the number of messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val timeStart = System.currentTimeMillis()
    val parquet = sparkSession.read.parquet(path)
    parquet.show(5,50)
    parquet.printSchema()
    val df = parquet
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
