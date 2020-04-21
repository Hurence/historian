package com.hurence.historian.spark.sql

import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverterOfRecord
import com.hurence.logisland.timeseries.sax.SaxConverter
import com.hurence.logisland.util.string.BinaryEncodingUtils
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.collection.JavaConverters._



// TODO : add functions on Dataset[ChunkRecordV0]
object functions {


  private val converter = new BinaryCompactionConverterOfRecord.Builder().build


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
    * Encoding function: returns the sax string of the values.
    */
  val sax = udf { (alphabetSize:Int, nThreshold:Float, paaSize:Int, values: mutable.WrappedArray[Double]) =>


    val saxConverter = new SaxConverter.Builder()
      .alphabetSize(alphabetSize)
      .nThreshold(nThreshold)
      .paaSize(paaSize)
      .build()

    val list = values.map(Double.box).asJava


    saxConverter.getSaxStringFromValues(list)

  }
}
