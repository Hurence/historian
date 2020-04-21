package com.hurence.historian


import com.hurence.historian.LoaderMode.Value
import com.hurence.historian.processor.{HistorianContext, TimeseriesConverter}
import com.hurence.historian.spark.compactor.ChunkCompactor
import com.hurence.logisland.record.{EvoaUtils, TimeseriesRecord}
import com.hurence.logisland.timeseries.MetricTimeSeries
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.spark.sql.functions.{asc, concat, from_unixtime, hour, lit}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory




class DataLoader extends Serializable {


  object DataLoaderMode extends Enumeration {
    type DataLoaderMode = Value

    val CSV_TO_PARQUET = Value("csv-to-parquet")
    val PARQUET_TO_SOLR = Value("parquet-to-solr")
  }





  private val logger = LoggerFactory.getLogger(classOf[ChunkCompactor])

  val DEFAULT_CHUNK_SIZE = 1440
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 100

  implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeseriesRecord]

  case class DataLoaderOptions(master: String,
                               zkHosts: String,
                               collectionName: String,
                               csvFilePath: scala.Option[String],
                               parquetFilePath: scala.Option[String],
                               chunkSize: Int,
                               saxAlphabetSize: Int,
                               saxStringLength: Int,
                               useKerberos: Boolean)

  val options = DataLoaderOptions("local[*]", "zookeeper:2181", "historian", None, None, 1440, 7, 100, useKerberos = false)

  def parseCommandLine(args: Array[String]): DataLoaderOptions = {

    val parser = new DefaultParser
    val options = new Options


    val helpMsg = "Print this message."
    val help = new Option("help", helpMsg)
    options.addOption(help)

    options.addOption(
      Option.builder("ms")
        .longOpt("spark-master")
        .hasArg(true)
        .desc("spark master")
        .build()
    )

    options.addOption(Option.builder("zk")
      .longOpt("zookeeper-quorum")
      .hasArg(true)
      .desc(s"the zookeeper quorum for solr collection")
      .build()
    )
    options.addOption(Option.builder("col")
      .longOpt("collection-name")
      .hasArg(true)
      .desc(s"Solr collection name, default historian")
      .build()
    )

    options.addOption(Option.builder("csv")
      .longOpt("csv-file-path")
      .hasArg(true)
      .desc(s"File path mask, can be anything like /a/B/c/*/*pr*/*.csv")
      .build()
    )

    options.addOption(Option.builder("pq")
      .longOpt("parquet-file-path")
      .hasArg(true)
      .desc(s"File path mask, can be anything like /a/B/c/*/*pr*/*.parquet")
      .build()
    )

    options.addOption(Option.builder("cs")
      .longOpt("chunks-size")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"num points in a chunk, default $DEFAULT_CHUNK_SIZE")
      .build()
    )

    options.addOption(Option.builder("sas")
      .longOpt("sax-alphabet-size")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"size of alphabet, default $DEFAULT_SAX_ALPHABET_SIZE")
      .build()
    )

    options.addOption(Option.builder("ssl")
      .longOpt("sax-string-length")
      .hasArg(true)
      .optionalArg(true)
      .desc(s"num points in a chunk, default $DEFAULT_SAX_STRING_LENGTH")
      .build()
    )

    options.addOption(Option.builder("kb")
      .longOpt("kerberos")
      .optionalArg(true)
      .desc("do we use kerberos ?, default false")
      .build()
    )

    options.addOption(Option.builder("date")
      .longOpt("recompaction-date")
      .hasArg(true)
      .optionalArg(true)
      .desc("the day date to recompact in the form of yyyy-MM-dd")
      .build()
    )

    // parse the command line arguments
    val line = parser.parse(options, args)
    val sparkMaster = if (line.hasOption("ms")) line.getOptionValue("ms") else "local[*]"
    val useKerberos = if (line.hasOption("kb")) true else false
    val zkHosts = if (line.hasOption("zk")) line.getOptionValue("zk") else "localhost:2181"
    val collectionName = if (line.hasOption("col")) line.getOptionValue("col") else "historian"
    val csvFilePath = if (line.hasOption("csv")) Some(line.getOptionValue("csv")) else None
    val parquetFilePath = if (line.hasOption("pq")) Some(line.getOptionValue("pq")) else None
    val chunksSize = if (line.hasOption("cs")) line.getOptionValue("chunks").toInt else DEFAULT_CHUNK_SIZE
    val alphabetSize = if (line.hasOption("sas")) line.getOptionValue("sa").toInt else DEFAULT_SAX_ALPHABET_SIZE
    val saxStringLength = if (line.hasOption("ssl")) line.getOptionValue("sl").toInt else DEFAULT_SAX_STRING_LENGTH

    // build the option handler
    val opts = DataLoaderOptions(sparkMaster,
      zkHosts,
      collectionName,
      csvFilePath,
      parquetFilePath,
      chunksSize,
      alphabetSize,
      saxStringLength,
      useKerberos)

    logger.info(s"Command line options : $opts")
    opts
  }


  /**
    * take a CSV file and make a PARQUET one well ordered and partionned by day
    *
    * @param spark
    * @param options
    * @param filterQuery
    */
  def fromCsvToParquet(spark: SparkSession, options: DataLoaderOptions, filterQuery: scala.Option[String]): Unit = {






    import spark.implicits._
    val data = List(
      ("2019-11-20",1638.6548611111111,"ebbbcbbbbcbbacbbbbbbbbabbbbbbcbccccdfeefefgffggggefdffdefefffcfffffgffeffffdeeededdcbccccccbabcccbbb"),
      ("2019-11-21",1657.3131944444435,"addabbbbdbbbabababbcababbbbbaabbbbcdfffgedgefffffffdfffffffffeegffefeefffffeffefffgdddeddddcddccdddd"),
      ("2019-11-22",1690.4874999999995,"bbaabbbdaaaabbbbccbcbabbbbabcaabbbfedfeffegedfeggggfgggggffgfeeffgfeffgffgfffeecefebccbbdddcdcdbcbaa"),
      ("2019-11-24",1625.320833333333, "abbaaabbbaaabbbbbbbbcbccbabcabccbbeeedffffffccgfggcffffeffeeeffffffggfedeffffggggegddecacbccccbcdbcc"),
      ("2019-11-25",1649.2555555555555,"cbbccccbbaeabcccbccbdecccccccbdcdfdeeeddeeceffeefffffcffffffffffffegffffdffdgeeeedebbbbbcbbcbbbbbbcb"),
      ("2019-11-26",1628.3833333333334,"fbbbcbcabbbcccaccbbbbbbaabbbbbbbbbacdgfeggffedeeeffefedafffgfffffffffffgfegefefffgfcbbcccccbbcccbbcc"),
      ("2019-11-27",1661.41875,        "bbbaaaabbbbabbbbacbbcdbcacbcbbbcbbdfggffgffgfffdffffgffegegffggfffgfcefggfgfffeedfecbcacbacccccbbacb"),
      ("2019-11-28",1643.9034965034962,"cbbcbcbbbbbbbbbbbcbbbbcbcccbbbcbcbcbbbbdeeeedgggggddddddddddddcdecfeddddddddddddddcddddddddddddddddd"))


    val df = data.toDF("day", "avg", "sax")



    df.map(r => (r.getString(0),
      r.getString(2).count(_ == 'a')/100.0,
      r.getString(2).count(_ == 'b')/100.0,
      r.getString(2).count(_ == 'c')/100.0,
      r.getString(2).count(_ == 'd')/100.0,
      r.getString(2).count(_ == 'e')/100.0,
      r.getString(2).count(_ == 'f')/100.0,
      r.getString(2).count(_ == 'g')/100.0))
      .collect().foreach(println)

    val csvOpts = Map(
      "inferSchema" -> "true",
      "delimiter" -> ";",
      "header" -> "true",
      "dateFormat" -> ""
    )
    val filePath = options.csvFilePath.get
    val outPath = options.parquetFilePath.get

    val ds = spark.read
      .format("csv")
      .options(csvOpts)
      .load(filePath)
      .withColumn("day", from_unixtime($"timestamp", "yyyy-MM-dd"))
      .withColumn("hour", hour(from_unixtime($"timestamp")))
      .withColumn("timestamp", $"timestamp" * 1000L)
      .withColumn("name", concat($"metric_name", lit("@"), $"metric_id"))
      .select("name", "timestamp", "value", "day", "hour", "warn", "crit",  "min", "max")

    val filteredDS = if (filterQuery.isDefined) ds.filter(filterQuery.get) else ds

    filteredDS
      .repartition($"day")
      .sortWithinPartitions(asc("name"), asc("timestamp"))
      .write
      .partitionBy("day")
      .mode("append")
      .parquet(outPath)

  }

  def saveNewChunksToSolR(timeseriesDS: Dataset[TimeseriesRecord], options: DataLoaderOptions) = {


    import timeseriesDS.sparkSession.implicits._

    val solrOpts = Map(
      "zkhost" -> options.zkHosts,
      "collection" -> options.collectionName
    )

    logger.info(s"start saving new chunks to ${options.collectionName}")
    val savedDF = timeseriesDS
      .map(r => (
        r.getId,
        r.getField("day").asString(),
        r.getField(TimeseriesRecord.METRIC_NAME).asString(),
        r.getField(TimeseriesRecord.CHUNK_VALUE).asString(),
        r.getField(TimeseriesRecord.CHUNK_START).asLong(),
        r.getField(TimeseriesRecord.CHUNK_END).asLong(),
        r.getField(TimeseriesRecord.CHUNK_WINDOW_MS).asLong(),
        r.getField(TimeseriesRecord.CHUNK_SIZE).asInteger(),
        r.getField(TimeseriesRecord.CHUNK_FIRST_VALUE).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_AVG).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_MIN).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_MAX).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_COUNT).asInteger(),
        r.getField(TimeseriesRecord.CHUNK_SUM).asDouble(),
        r.getField(TimeseriesRecord.CHUNK_TREND).asBoolean(),
        r.getField(TimeseriesRecord.CHUNK_OUTLIER).asBoolean(),
        if (r.hasField(TimeseriesRecord.CHUNK_SAX)) r.getField(TimeseriesRecord.CHUNK_SAX).asString() else "",
        r.getField(TimeseriesRecord.CHUNK_ORIGIN).asString())
      )
      .toDF("id",
        "day",
        TimeseriesRecord.METRIC_NAME,
        TimeseriesRecord.CHUNK_VALUE,
        TimeseriesRecord.CHUNK_START,
        TimeseriesRecord.CHUNK_END,
        TimeseriesRecord.CHUNK_WINDOW_MS,
        TimeseriesRecord.CHUNK_SIZE,
        TimeseriesRecord.CHUNK_FIRST_VALUE,
        TimeseriesRecord.CHUNK_AVG,
        TimeseriesRecord.CHUNK_MIN,
        TimeseriesRecord.CHUNK_MAX,
        TimeseriesRecord.CHUNK_COUNT,
        TimeseriesRecord.CHUNK_SUM,
        TimeseriesRecord.CHUNK_TREND,
        TimeseriesRecord.CHUNK_OUTLIER,
        TimeseriesRecord.CHUNK_SAX,
        TimeseriesRecord.CHUNK_ORIGIN)

    savedDF.write
      .format("solr")
      .options(solrOpts)
      .save()

    // Explicit commit to make sure all docs are visible
    val solrCloudClient = SolrSupport.getCachedCloudClient(options.zkHosts)
    val response = solrCloudClient.commit(options.collectionName, true, true)
    logger.info(s"done saving new chunks : ${response.toString}")

    savedDF
  }


  def makeChunksFromParquet(spark: SparkSession, options: DataLoaderOptions): Dataset[TimeseriesRecord] = {

    import spark.implicits._
    implicit val tsrEncoder = org.apache.spark.sql.Encoders.kryo[TimeseriesRecord]

    spark.read.parquet(options.parquetFilePath.get)
      .withColumn("day", from_unixtime($"timestamp" / 1000, "yyyy-MM-dd"))
      .map(r => (
        (r.getAs[String]("day"), r.getAs[String]("name")),
        List((r.getAs[Long]("timestamp"), r.getAs[Double]("value")))))
      .rdd
      .reduceByKey((g1, g2) => g1 ::: g2)
      .map(r => (r._1, r._2.sortWith((l1, l2) => l1._1 < l2._1).grouped(1440).toList))
      .mapPartitions(p => {

        if (p.nonEmpty) {
          // Init the Timeserie processor
          val tsProcessor = new TimeseriesConverter()
          val context = new HistorianContext(tsProcessor)
          context.setProperty(TimeseriesConverter.GROUPBY.getName, TimeseriesRecord.METRIC_NAME)
          context.setProperty(TimeseriesConverter.METRIC.getName,
            s"first;min;max;count;sum;avg;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")

          tsProcessor.init(context)


          p.flatMap(mergedRecord => {


            mergedRecord._2.map(l => {

              val day = mergedRecord._1._1
              val name = mergedRecord._1._2
              val tsBuilder = new MetricTimeSeries.Builder(name, "rd-booster")

            /*  tsBuilder.attribute("metric_id", metricId)
              tsBuilder.attribute("metric_name", metricName)*/
              tsBuilder.attribute("day", day)

              l.foreach(t => tsBuilder.point(t._1, t._2))

              val record = new TimeseriesRecord(tsBuilder.build())



              tsProcessor.computeValue(record)
              tsProcessor.computeMetrics(record)
              EvoaUtils.setHashId(record)
              EvoaUtils.setChunkOrigin(record, TimeseriesRecord.CHUNK_ORIGIN_LOADER)
              record
            })

          })
        } else
          Iterator.empty
      })
      .toDS()
  }

}

object DataLoader {
  /**
    *
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val dataLoader = new DataLoader()

    // get arguments
    val options = dataLoader.parseCommandLine(args)

    // setup spark session
    val spark = SparkSession.builder
      .appName("DataLoader")
      .master(options.master)
      .getOrCreate()


    if (options.csvFilePath.isDefined && options.parquetFilePath.isDefined) {

      dataLoader.fromCsvToParquet(spark, options, None)

    } else if (options.parquetFilePath.isDefined) {

      val mergedTimeseriesDS = dataLoader.makeChunksFromParquet(spark, options)
      dataLoader.saveNewChunksToSolR(mergedTimeseriesDS, options)
    }


    spark.close()
  }

}
