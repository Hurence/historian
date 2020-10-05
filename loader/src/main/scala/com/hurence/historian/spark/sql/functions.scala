package com.hurence.historian.spark.sql

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.stream.Collectors

import com.google.common.hash.Hashing
import com.hurence.historian.date.util.DateUtil
import com.hurence.timeseries.MetricTimeSeries
import com.hurence.timeseries.analysis.{TimeseriesAnalysis, TimeseriesAnalyzer}
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesWithQualitySerializer
import com.hurence.timeseries.compaction.{BinaryCompactionUtil, BinaryEncodingUtils, Compression}
import com.hurence.timeseries.model.Measure
import com.hurence.timeseries.sax.{GuessSaxParameters, SaxAnalyzer, SaxConverter}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal


object functions {

  val sha256 = udf { toHash: String =>
    Hashing.sha256.hashString(toHash, StandardCharsets.UTF_8).toString
  }

  val toDateUTC = udf { (epochMilliUTC: Long, dateFormat: String) =>
    val dateFormatter = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
      .withZone(java.time.ZoneId.of("Europe/Paris"))

    try {
      dateFormatter.format(java.time.Instant.ofEpochMilli(epochMilliUTC))
    } catch {
      case NonFatal(_) => null
    }
  }

  val toTimestampUTC = udf { (dateString: String, dateFormat: String) =>
    try {
      DateUtil.parse(dateString, dateFormat).getTime
    } catch {
      case NonFatal(_) => 0L
    }
  }


  /**
    * Encoding function: returns the base64 encoding as a Chronix chunk.
    */
  val chunk = udf { (name: String,
                     start: Long,
                     end: Long,
                     timestamps: mutable.WrappedArray[Long],
                     values: mutable.WrappedArray[Double],
                     qualities: mutable.WrappedArray[Float]) =>

    val builder = new MetricTimeSeries.Builder(name)
      .start(start)
      .end(end)

    // (timestamps zip values).map { case (t, v) => builder.point(t, v) }
    (timestamps, values, qualities).zipped.map { case (t, v, q) => builder.point(t, v, q) }

    BinaryCompactionUtil.serializeTimeseries(builder.build())
    // BinaryEncodingUtils.encode(bytes)

  }


  /**
    * Decoding function: returns the base64 encoding as a Chronix chunk.
    */
  val unchunk = udf { (bytes: Array[Byte], start: Long, end: Long) =>


    BinaryCompactionUtil.unCompressPoints(bytes, start, end)
      .stream()
      .collect(Collectors.toList())
      .asScala
      .map(p => (p.getTimestamp, p.getValue, p.getQuality, p.getDay))

    /*val bytes = BinaryEncodingUtils.decode(chunk)

    BinaryCompactionUtil.unCompressPoints(bytes, start, end)
      .stream()
      .collect(Collectors.toList())
      .asScala
      .map(p => (p.getTimestamp, p.getValue, p.getQuality, p.getDay))
  */
  }


  val toBase64 = udf { (binaryChunk: Array[Byte]) => BinaryEncodingUtils.encode(binaryChunk) }
  val fromBase64 = udf { (stringChunk: String) => BinaryEncodingUtils.decode(stringChunk) }


  /**
    * Encoding function: returns the sax string of the values.
    */
  val sax = udf {
    (alphabetSize: Int, nThreshold: Float, paaSize: Int, values: mutable.WrappedArray[Double]) =>


      val saxConverter = new SaxConverter.Builder()
        .alphabetSize(alphabetSize)
        .nThreshold(nThreshold)
        .paaSize(paaSize)
        .build()

      val list = values.map(Double.box).asJava


      saxConverter.getSaxStringFromValues(list)

  }


  case class Analysis(min: Double, max: Double, avg: Double, stdDev: Double, sum: Double, trend: Boolean, outlier: Boolean, count: Long, first: Double, last: Double)

  /**
    * Encoding function: returns the sax string of the values.
    */
  val analysis = udf {
    (timestamps: mutable.WrappedArray[Long], values: mutable.WrappedArray[Double]) =>


      val analyzer = TimeseriesAnalyzer.builder().build()
      val timestampsList = timestamps.map(Long.box).asJava
      val valuesList = values.map(Double.box).asJava
      val analysis = analyzer.run(timestampsList, valuesList)

      Analysis(analysis.getMin,
        analysis.getMax,
        analysis.getMean,
        analysis.getStdDev,
        analysis.getSum,
        analysis.isHasTrend,
        analysis.isHasOutlier,
        analysis.getCount,
        analysis.getFirst,
        analysis.getLast)
  }

  /**
    * Best guess function: returns the best guess parameters.
    *
    *
    *
    */
  val guess = udf {
    (values: mutable.WrappedArray[Double]) =>

      val list = values.map(Double.box).asJava

      GuessSaxParameters.computeBestParam(list).toString

  }

  /**
    * Encoding function: returns the sax string of the values using the best guess.
    */
  val sax_best_guess = udf {
    (nThreshold: Float, values: mutable.WrappedArray[Double]) =>

      val list = values.map(Double.box).asJava
      val best_guess = GuessSaxParameters.computeBestParam(list).asScala.toList
      val paaSize = best_guess(1).toString.toInt
      val alphabetSize = best_guess(2).toString.toInt

      val saxConverter = new SaxConverter.Builder()
        .alphabetSize(alphabetSize)
        .nThreshold(nThreshold)
        .paaSize(paaSize)
        .build()

      saxConverter.getSaxStringFromValues(list)

  }
  val sax_best_guess_paa_fixed = udf {
    (nThreshold: Float, paaSize: Int, values: mutable.WrappedArray[Double]) =>

      val list = values.map(Double.box).asJava
      val best_guess = GuessSaxParameters.computeBestParam(list).asScala.toList
      //val paaSize =best_guess(1).toString.toInt
      val alphabetSize = best_guess(2).toString.toInt

      val saxConverter = new SaxConverter.Builder()
        .alphabetSize(alphabetSize)
        .nThreshold(nThreshold)
        .paaSize(paaSize)
        .build()

      saxConverter.getSaxStringFromValues(list)

  }

  /**
    * Encoding function: returns the sax string of the values using the best guess.
    */
  val anomalie_test = udf {
    (str: String) =>

      val saxThreshold = SaxAnalyzer.saxThreshold(str, 0.1)
      SaxAnalyzer.anomalyDetect(str, saxThreshold).toString

  }

  /**
    * Reorders columns as specified
    * Reorders the columns in a DataFrame.
    *
    * {{{
    * val actualDF = sourceDF.reorderColumns(
    *   Seq("greeting", "team", "cats")
    * )
    * }}}
    *
    * The `actualDF` will have the `greeting` column first, then the `team` column then the `cats` column.
    */
  def reorderColumns(df: DataFrame, colNames: Seq[String]): DataFrame = {
    val cols = colNames.map(col(_))
    df.select(cols: _*)
  }
}
