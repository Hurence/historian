package com.hurence.historian.spark.sql

import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverterOfRecord
import com.hurence.logisland.timeseries.sax.{GuessSaxParameters, SaxConverter}
import com.hurence.logisland.util.DateUtil
import com.hurence.logisland.util.string.BinaryEncodingUtils
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.control.NonFatal


object functions {

  private val converter = new BinaryCompactionConverterOfRecord.Builder().build


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
    * Encoding function: returns the sax string of the values.
    *
    *
    *
    */
  val guess = udf { (alphabetSize: Int, values: mutable.WrappedArray[Double]) =>


    val guessSasxParams = new GuessSaxParameters.Builder()
      .alphabetSize(alphabetSize)
      .build()

    val list = values.map(Double.box).asJava


    guessSasxParams.computeBestParam(list).asScala

    // mutable.WrappedArray(guessParm)

  }
}
