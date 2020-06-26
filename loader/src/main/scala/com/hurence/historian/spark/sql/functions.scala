package com.hurence.historian.spark.sql

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import com.hurence.logisland.BinaryCompactionConverterOfRecord
import com.hurence.logisland.timeseries.sax.SaxConverter
import com.hurence.logisland.util.DateUtil
import com.hurence.timeseries.MetricTimeSeries
import com.hurence.timeseries.compaction.BinaryEncodingUtils
import com.hurence.timeseries.sax.{GuessSaxParameters, SaxAnalyzer, SaxConverter}
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal


object functions {

  private val converter = new BinaryCompactionConverterOfRecord.Builder().build


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
  val chunk = udf { (name: String, start: Long, end: Long, timestamps: mutable.WrappedArray[Long], values: mutable.WrappedArray[Double]) =>

    // @TODO move this into timeseries modules and do the same for Chronix functions call
    val builder = new MetricTimeSeries.Builder(name, "measures")
      .start(start)
      .end(end)

    (timestamps zip values).map { case (t, v) => builder.point(t, v) }

    val bytes = converter.serializeTimeseries(builder.build())
    BinaryEncodingUtils.encode(bytes)

  }


  /**
    * Decoding function: returns the base64 encoding as a Chronix chunk.
    */
  val unchunk = udf { (chunk: String, start: Long, end: Long) =>


    val bytes = BinaryEncodingUtils.decode(chunk)

    converter.deSerializeTimeseries(bytes, start, end).asScala.map(p => (p.getTimestamp, p.getValue))


  }

  /**
    * Encoding function: returns the sax string of the values.
    */
  val sax = udf { (alphabetSize: Int, nThreshold: Float, paaSize: Int, values: mutable.WrappedArray[Double]) =>


    val saxConverter = new SaxConverter.Builder()
      .alphabetSize(alphabetSize)
      .nThreshold(nThreshold)
      .paaSize(paaSize)
      .build()

    val list = values.map(Double.box).asJava


    saxConverter.getSaxStringFromValues(list)

  }


  /**
    * Best guess function: returns the best guess parameters.
    *
    *
    *
    */
  val guess = udf { ( values: mutable.WrappedArray[Double]) =>

    val list = values.map(Double.box).asJava

    GuessSaxParameters.computeBestParam(list).toString

  }

  /**
   * Encoding function: returns the sax string of the values using the best guess.
   */
  val sax_best_guess = udf { ( nThreshold: Float ,values: mutable.WrappedArray[Double]) =>

    val list = values.map(Double.box).asJava
    val best_guess = GuessSaxParameters.computeBestParam(list).asScala.toList
    val paaSize =best_guess(1).toString.toInt
    val alphabetSize = best_guess(2).toString.toInt

    val saxConverter = new SaxConverter.Builder()
      .alphabetSize(alphabetSize)
      .nThreshold(nThreshold)
      .paaSize(paaSize)
      .build()

    saxConverter.getSaxStringFromValues(list)

  }
  val sax_best_guess_paa_fixed = udf { ( nThreshold: Float, paaSize: Int ,values: mutable.WrappedArray[Double]) =>

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
  val anomalie_test = udf { ( str: String) =>

    val saxThreshold =  SaxAnalyzer.saxThreshold(str,0.1)
    SaxAnalyzer.anomalyDetect(str,saxThreshold).toString

  }
}
