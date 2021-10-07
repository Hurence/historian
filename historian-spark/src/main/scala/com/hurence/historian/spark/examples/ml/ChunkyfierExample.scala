package com.hurence.historian.spark.examples.ml


// $example on$
import java.util.Date

import com.hurence.historian.spark.ml.{Chunkyfier, UnChunkyfier}

// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example for Chunkyfier.
  * Run with
  * {{{
  * bin/run-example ml.BucketizerExample
  * }}}
  */
object ChunkyfierExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ChunkyfierExample")
      .getOrCreate()

    // $example on$
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val now = new Date().getTime
    val oneSec = 1000L
    val oneMin = 60 * oneSec
    val oneHour = 60 * oneMin
    val oneDay = 24 * oneHour

    val dataFrame = spark.createDataFrame(Seq(
      ("metric_a", 1.3, now + 30 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.2, now + 20 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.1, now + 10 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.4, now + 40 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.5, now + 50 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.6, now + oneHour, Map("metric_id" -> 1)),
      ("metric_a", 1.7, now + oneDay + 10 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.8, now + oneDay + 20 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 2.1, now + 10 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.2, now + 20 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.3, now + 30 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.4, now + oneHour, Map("metric_id" -> 2)),
      ("metric_b", 3.1, now + 10 * oneMin, Map("metric_id" -> 3)),
      ("metric_b", 3.2, now + 20 * oneMin, Map("metric_id" -> 3))
    ))
      .toDF( "name", "value", "timestamp", "tags")

    dataFrame.show()


    /*
+--------+-----+-------------+----------------+
|    name|value|    timestamp|            tags|
+--------+-----+-------------+----------------+
|metric_a|  1.3|1588094307365|[metric_id -> 1]|
|metric_a|  1.2|1588093707365|[metric_id -> 1]|
|metric_a|  1.1|1588093107365|[metric_id -> 1]|
|metric_a|  1.4|1588094907365|[metric_id -> 1]|
|metric_a|  1.5|1588095507365|[metric_id -> 1]|
|metric_a|  1.6|1588096107365|[metric_id -> 1]|
|metric_a|  1.7|1588179507365|[metric_id -> 1]|
|metric_a|  1.8|1588180107365|[metric_id -> 1]|
|metric_a|  2.1|1588093107365|[metric_id -> 2]|
|metric_a|  2.2|1588093707365|[metric_id -> 2]|
|metric_a|  2.3|1588094307365|[metric_id -> 2]|
|metric_a|  2.4|1588096107365|[metric_id -> 2]|
|metric_b|  3.1|1588093107365|[metric_id -> 3]|
|metric_b|  3.2|1588093707365|[metric_id -> 3]|
+--------+-----+-------------+----------------+
     */

    val chunkyfier = new Chunkyfier()
      .setValueCol("value")
      .setTimestampCol("timestamp")
      .setDateBucketFormat("yyyy-MM-dd")
      .setChunkMaxSize(1440)
      .setSaxAlphabetSize(4)
      .setSaxStringLength(4)


    // Transform original data into its bucket index.
    val bucketedData = chunkyfier.transform(dataFrame)
    bucketedData.show()


    /*
+----------+--------+---------+----------------+-------------+-------------+-----+---+---+-----+----+-------------------+------------------+--------------------+---------+
|       day|    name|metric_id|            tags|        start|          end|count|min|max|first|last|             std_dev|               avg|               chunk|      sax|
+----------+--------+---------+----------------+-------------+-------------+-----+---+---+-----+----+-------------------+------------------+--------------------+---------+
|2020-04-28|metric_a|        2|[metric_id -> 2]|1588088793377|1588091793377|    4|2.1|2.4|  2.1| 2.4|0.12909944487358033|              2.25|H4sIAAAAAAAAAOPi1...|     abcd|
|2020-04-28|metric_b|        3|[metric_id -> 3]|1588088793377|1588089393377|    2|3.1|3.2|  3.1| 3.2|0.07071067811865482|3.1500000000000004|H4sIAAAAAAAAAOPi1...|sax_error|
|2020-04-28|metric_a|        1|[metric_id -> 1]|1588088793377|1588091793377|    6|1.1|1.6|  1.1| 1.6|0.18708286933869708|1.3499999999999999|H4sIAAAAAAAAAOPi1...|     abcd|
|2020-04-29|metric_a|        1|[metric_id -> 1]|1588175193377|1588175793377|    2|1.7|1.8|  1.7| 1.8|0.07071067811865482|              1.75|H4sIAAAAAAAAAOPi1...|sax_error|
+----------+--------+---------+----------------+-------------+-------------+-----+---+---+-----+----+-------------------+------------------+--------------------+---------+
 */



    val unchunkyfier = new UnChunkyfier()
    unchunkyfier.transform(bucketedData).show()

    /*
+--------+-----+-------------+----------------+
|    name|value|    timestamp|            tags|
+--------+-----+-------------+----------------+
|metric_a|  2.1|1588093212579|[metric_id -> 2]|
|metric_a|  2.2|1588093812579|[metric_id -> 2]|
|metric_a|  2.3|1588094412579|[metric_id -> 2]|
|metric_a|  2.4|1588096212579|[metric_id -> 2]|
|metric_b|  3.1|1588093212579|[metric_id -> 3]|
|metric_b|  3.2|1588093812579|[metric_id -> 3]|
|metric_a|  1.1|1588093212579|[metric_id -> 1]|
|metric_a|  1.2|1588093812579|[metric_id -> 1]|
|metric_a|  1.3|1588094412579|[metric_id -> 1]|
|metric_a|  1.4|1588095012579|[metric_id -> 1]|
|metric_a|  1.5|1588095612579|[metric_id -> 1]|
|metric_a|  1.6|1588096212579|[metric_id -> 1]|
|metric_a|  1.7|1588179612579|[metric_id -> 1]|
|metric_a|  1.8|1588180212579|[metric_id -> 1]|
+--------+-----+-------------+----------------+
     */

    spark.stop()
  }
}

// scalastyle:on println

