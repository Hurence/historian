package com.hurence.historian

import com.hurence.historian.App.logger
import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.record.{FieldDictionary, TimeSeriesRecord}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


class ChunkTagger extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[ChunkTagger])

  def process(options: LoaderOptions, spark: SparkSession): Unit = {

    import spark.implicits._


    val pattern = "(.+):((.+)\\/year=(.+)\\/month=(.+)\\/week=(.+)\\/code_install=(.+)\\/name=(.+)\\/(.+))".r
    val tsDF = spark.sparkContext.wholeTextFiles(options.in).map(r => {


      try {
        val filePath = r._1
        val pathTokens = pattern.findAllIn(filePath).matchData.toList.head

        logger.info(s"processing $filePath")

        val year = pathTokens.group(4).toInt
        val month = pathTokens.group(5).toInt
        val week = pathTokens.group(6).toInt
        val codeInstall = pathTokens.group(7)
        val name = pathTokens.group(8)

        val measures = r._2
          .split("\n")
          .tail
          .map(line => {
            try {
              val lineTokens = line.split(",")
              val value = lineTokens(0).toDouble
              val quality = lineTokens(1).toDouble
              val sensor = lineTokens(2)
              val timestamp = lineTokens(3)
              val timeMs = lineTokens(4).toLong
              val day = lineTokens(5).toInt

              Some(EvoaMeasure(name, codeInstall, sensor, value, quality, timestamp, timeMs, year, month, week, day, filePath))
            } catch {
              case _: Throwable =>
                None
            }

          })
          .filter(_.isDefined)
          .map(_.get)
          .sortBy(_.timeMs)

        Some((name, measures))
      } catch {
        case _: Throwable =>
          None
      }


    })
      .filter(_.isDefined)
      .map(_.get)
      .flatMap(m => {

        val name = m._1
        val measures = m._2

        // Init the Timeserie processor
        val tsProcessor = new TimeseriesConverter()
        val context = new HistorianContext(tsProcessor)
        context.setProperty(TimeseriesConverter.GROUPBY.getName, "name")
        context.setProperty(TimeseriesConverter.METRIC.getName,
          s"first;sum;min;max;avg;trend;outlier;sax:${options.saxAlphabetSize},0.005,${options.saxStringLength}")
        tsProcessor.init(context)

        // Slide over each group
        measures.sliding(options.chunkSize, options.chunkSize)
          .map(subGroup => tsProcessor.fromMeasurestoTimeseriesRecord(subGroup.toList))
          /* .map(ts => (
             if(ts.getId!=null) ts.getId else "",
             if(ts.hasField(TimeSeriesRecord.CHUNK_SAX))
               ts.getField(TimeSeriesRecord.CHUNK_SAX).asString()
             else
               "",
             if(ts.hasField(TimeSeriesRecord.CHUNK_START)) ts.getField(TimeSeriesRecord.CHUNK_START).asLong() else 0L,
             if(ts.hasField(FieldDictionary.RECORD_NAME))  ts.getField(FieldDictionary.RECORD_NAME).asString() else ""))*/

          .map(ts => (ts.getId,
          ts.getField(TimeSeriesRecord.CHUNK_START).asLong(),
          ts.getField(FieldDictionary.RECORD_NAME).asString(),
          if (ts.hasField(TimeSeriesRecord.CHUNK_SAX)) ts.getField(TimeSeriesRecord.CHUNK_SAX).asString() else ""
        ))
      })
      .toDF("key", "start", "name", "sax")


    // TODO : manipulate DAtaframe here to find some anomaly in the string
    // levenstein distance ?

    // aaaaaaaaaaaaaaaadbbbbbbbb  => anomalyIndex = 14
     tsDF.show(10)

    tsDF .write
      .mode(SaveMode.Append)
      .format("csv")
      .option("header", "true")
      .save(options.out)
  }

}
