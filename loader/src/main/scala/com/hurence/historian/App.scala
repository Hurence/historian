package com.hurence.historian


import java.util.Calendar

import com.hurence.historian.LoaderMode.LoaderMode
import com.hurence.historian.SchemaAnalysis.getType
import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.record.{FieldDictionary, Record, RecordDictionary, StandardRecord, TimeSeriesRecord}
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter
import org.apache.commons.cli.{CommandLine, GnuParser, Option, OptionBuilder, Options, Parser}
import org.apache.spark.sql.{Encoder, Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.io.Source

case class EvoaMeasure(name: String,
                       codeInstall: String,
                       sensor: String,
                       value: Double,
                       quality: Double,
                       timestamp: String,
                       timeMs: Long,
                       year: Int,
                       month: Int,
                       day: Int,
                       filePath: String)


object LoaderMode extends Enumeration {
  type LoaderMode = Value

  val PRELOAD = Value("preload")
  val CHUNK = Value("chunk")
  val CHUNK_BY_FILE = Value("chunk_by_file")
}


case class LoaderOptions(mode: LoaderMode, in: String, out: String, master: String, appName: String, brokers: scala.Option[String], lookup: scala.Option[String], chunkSize: Int, schema: scala.Option[String], saxAlphabetSize:Int, saxStringLength: Int, useKerberos: Boolean)


/**
  * @author Thomas Bailet @Hurence
  */
object App {

  private val logger = LoggerFactory.getLogger(classOf[App])

  val DEFAULT_CHUNK_SIZE = 2000
  val DEFAULT_SAX_ALPHABET_SIZE = 7
  val DEFAULT_SAX_STRING_LENGTH = 100

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def parseCommandLine(args: Array[String]): LoaderOptions = {
    // Commande lien management
    val parser = new GnuParser
    val options = new Options


    val helpMsg = "Print this message."
    val help = new Option("help", helpMsg)
    options.addOption(help)

    // Mode
    OptionBuilder.withArgName("mode")
    OptionBuilder.withLongOpt("loading-mode")
    OptionBuilder.isRequired
    OptionBuilder.hasArg
    OptionBuilder.withDescription("preload or chunk : preload takes local csv files from input-path and partition data into year/month/code_install/name output-path whereas chunk takes hdfs input-path preloaded data and chunk it to kafka")
    val mode = OptionBuilder.create("mode")
    options.addOption(mode)

    // Input Path
    OptionBuilder.withArgName("in")
    OptionBuilder.withLongOpt("input-path")
    OptionBuilder.isRequired
    OptionBuilder.hasArg
    OptionBuilder.withDescription("input local folder for preload mode and input hdfs folder for chunk mode")
    val in = OptionBuilder.create("in")
    options.addOption(in)

    // Output Path
    OptionBuilder.withArgName("out")
    OptionBuilder.withLongOpt("output-path")
    OptionBuilder.isRequired
    OptionBuilder.hasArg
    OptionBuilder.withDescription("output local folder for preload mode and output kafka topic for chunk mode")
    val out = OptionBuilder.create("out")
    options.addOption(out)

    // Kafka brokers
    OptionBuilder.withArgName("brokers")
    OptionBuilder.withLongOpt("kafka-brokers")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("kafka brokers for chunk mode")
    val brokers = OptionBuilder.create("brokers")
    options.addOption(brokers)

    // Spark master
    OptionBuilder.withArgName("master")
    OptionBuilder.withLongOpt("spark-master")
    OptionBuilder.hasArg
    OptionBuilder.isRequired
    OptionBuilder.withDescription("spark master")
    val master = OptionBuilder.create("master")
    options.addOption(master)

    // Lookup path
    OptionBuilder.withArgName("lookup")
    OptionBuilder.withLongOpt("lookup-path")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("csv lookup ath for join")
    val lookup = OptionBuilder.create("lookup")
    options.addOption(lookup)

    // Chunk size
    OptionBuilder.withArgName("chunks")
    OptionBuilder.withLongOpt("chunks-size")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("num points in a chunk, default 2000")
    val chunk = OptionBuilder.create("chunks")
    options.addOption(chunk)

    // Input data schema file (for preload only)
    OptionBuilder.withArgName("schema")
    OptionBuilder.withLongOpt("schema-path")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("Path to the data schema file (for preload only)")
    val schema = OptionBuilder.create("schema")
    options.addOption(schema)

    // SAX Alphabet size
    OptionBuilder.withArgName("sa")
    OptionBuilder.withLongOpt("sax-alphabet-size")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("size of alphabet, default 7")
    val sa = OptionBuilder.create("sa")
    options.addOption(sa)

    // SAX String length
    OptionBuilder.withArgName("sl")
    OptionBuilder.withLongOpt("sax-length")
    OptionBuilder.hasArg
    OptionBuilder.withDescription("num points in a chunk, default 100")
    val sl = OptionBuilder.create("sl")
    options.addOption(sl)

    // SAX String length
    OptionBuilder.withArgName("kb")
    OptionBuilder.withLongOpt("kerberos")
    OptionBuilder.withDescription("do we use kerberos ?")
    val kb = OptionBuilder.create("kb")
    options.addOption(kb)

    // parse the command line arguments
    val line = parser.parse(options, args)
    val loadingMode = LoaderMode.withName(line.getOptionValue("mode").toLowerCase)
    val inputPath = line.getOptionValue("in")
    val outputPath = line.getOptionValue("out")
    val sparkMaster = line.getOptionValue("master")
    val useKerberos = if (line.hasOption("kb")) true else false
    val lookupPath = if (line.hasOption("lookup")) Some(line.getOptionValue("lookup")) else None
    val kafkaBrokers = if (line.hasOption("brokers")) Some(line.getOptionValue("brokers")) else None
    val chunksSize = if (line.hasOption("chunks")) line.getOptionValue("chunks").toInt else DEFAULT_CHUNK_SIZE
    val schemaPath = if (line.hasOption("schema")) Some(line.getOptionValue("schema")) else None
    val alphabetSize = if (line.hasOption("sa")) line.getOptionValue("sa").toInt else DEFAULT_SAX_ALPHABET_SIZE
    val saxStringLength = if (line.hasOption("sl")) line.getOptionValue("sl").toInt else DEFAULT_SAX_STRING_LENGTH


    /*if (loadingMode == LoaderMode.CHUNK && kafkaBrokers.isEmpty)
      throw new IllegalArgumentException(s"kafka broker must not be empty with $loadingMode mode")*/
    // add mandatory schema for preload
    /*if (loadingMode == LoaderMode.PRELOAD && schemaPath.isEmpty)
      throw new IllegalArgumentException(s"input schema structure must not be empty with $loadingMode mode")*/

    val appName = loadingMode match {
      case LoaderMode.PRELOAD => "EvoaPreloader"
      case LoaderMode.CHUNK => "EvoaChunker"
      case LoaderMode.CHUNK_BY_FILE => "EvoaChunkerByFile"
      case _ => throw new IllegalArgumentException(s"unknown $loadingMode mode")
    }

    LoaderOptions(loadingMode, inputPath, outputPath, sparkMaster, appName, kafkaBrokers, lookupPath, chunksSize, schemaPath, alphabetSize, saxStringLength, useKerberos)


  }

  def preload_IFPEN(options: LoaderOptions, spark: SparkSession): Unit = {

    import spark.implicits._

    // Define formats
    val csvRegexp = "((\\w+)\\.?(\\w+-?\\w+-?\\w+)?\\.?(\\w+)?)"
    val dateFmt = "dd/MM/yyyy HH:mm:ss"

    // Load raw data
    val rawMeasuresDF = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(options.in)

    // Then sort and split columns
    val measuresDF = if (options.lookup.isDefined) {
      val lookupDF = spark.read.format("csv")
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(options.lookup.get)

      rawMeasuresDF.join(lookupDF, "tagname")
        .withColumn("time_ms", unix_timestamp($"timestamp", dateFmt) * 1000)
        .withColumn("year", year(to_date($"timestamp", dateFmt)))
        .withColumn("month", month(to_date($"timestamp", dateFmt)))
        .withColumn("day", dayofmonth(to_date($"timestamp", dateFmt)))
        .withColumn("name", regexp_extract($"tagname", csvRegexp, 1))
        .withColumn("code_install", regexp_extract($"tagname", csvRegexp, 2))
        .withColumn("sensor", regexp_extract($"tagname", csvRegexp, 3))
        .withColumn("numeric_type", regexp_extract($"tagname", csvRegexp, 4))
        .select("name", "value", "quality", "code_install", "sensor", "numeric_type", "description", "engunits", "timestamp", "time_ms", "year", "month", "day")
        .sort(asc("name"), asc("time_ms"))
      // .dropDuplicates()
    } else {

      rawMeasuresDF
        .withColumn("time_ms", unix_timestamp($"timestamp", dateFmt) * 1000)
        .withColumn("year", year(to_date($"timestamp", dateFmt)))
        .withColumn("month", month(to_date($"timestamp", dateFmt)))
        .withColumn("day", dayofmonth(to_date($"timestamp", dateFmt)))
        .withColumn("name", regexp_extract($"tagname", csvRegexp, 1))
        .withColumn("code_install", regexp_extract($"tagname", csvRegexp, 2))
        .withColumn("sensor", regexp_extract($"tagname", csvRegexp, 3))
        .withColumn("numeric_type", regexp_extract($"tagname", csvRegexp, 4))
        .select("name", "value", "quality", "code_install", "sensor", "timestamp", "time_ms", "year", "month", "day")
        //  .dropDuplicates()
        .orderBy(asc("name"), asc("time_ms"))
    }

    // save this to output path
    measuresDF
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "code_install", "name")
      .format("csv")
      .option("header", "true")
      .save(options.out)


    println("IFPEN Preloading done")
  }


  def getType(raw: String): DataType = {
    raw match {
      case "ByteType" => ByteType
      case "ShortType" => ShortType
      case "IntegerType" => IntegerType
      case "LongType" => LongType
      case "FloatType" => FloatType
      case "DoubleType" => DoubleType
      case "BooleanType" => BooleanType
      case "TimestampType" => TimestampType
      case _ => StringType
    }
  }
  val mandatoryFields = List("value", "timestamp", "name")


  // Generic
  def preload(options: LoaderOptions, spark: SparkSession): Unit = {

    var structure = new StructType()
    Source.fromFile(options.schema.get).getLines().toList
      .flatMap(_.split(",")).map(_.replaceAll("\"", "").split(" "))
      .foreach(x => { if (mandatoryFields.contains(x(0))) structure = structure.add(x(0), getType(x(1)), false) else structure = structure.add(x(0), getType(x(1)), true) })
      //.map(x => structure = structure.add(x(0), getType(x(1)), true))


    import spark.implicits._

    // Load raw data
    val rawMeasuresDF = spark.read.format("csv")
      .option("sep", ";")
      //.option("inferSchema", "true")
      .option("header", "true")
      .schema(structure)
      .load(options.in)

    var colsToSelect = structure.fieldNames
    colsToSelect = colsToSelect :+ "time_ms" :+ "year" :+ "month" :+ "day"

    // Then sort and split columns
    val measuresDF = rawMeasuresDF
      .withColumn("time_ms", unix_timestamp(to_timestamp($"timestamp")) * 1000)
      .withColumn("year", year(to_date(to_timestamp($"timestamp"))))
      .withColumn("month", month(to_date(to_timestamp($"timestamp"))))
      .withColumn("day", dayofmonth(to_date(to_timestamp($"timestamp"))))
      .select(colsToSelect.head, colsToSelect.tail:_*)
      .sort(asc("name"), asc("time_ms"))
      .dropDuplicates()

    measuresDF.show()

    // save this to output path
    measuresDF
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day", "name")
      .format("csv")
      .option("header", "true")
      .save(options.out)


    println("Preloading done")
  }

  def chunk(options: LoaderOptions, spark: SparkSession): Unit = {

    import spark.implicits._

    val testDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(options.in)
      .cache()

    val chunkStructure = testDF.schema


    println("BEGINNING OF PROCESSING : ", format.format(Calendar.getInstance().getTime()))

    //val n = testDF.agg(countDistinct($"name")).collect().head.getLong(0).toInt

    implicit val enc: Encoder[TimeSeriesRecord] = org.apache.spark.sql.Encoders.kryo[TimeSeriesRecord]


    val tsDF = testDF.repartition($"name")
      //.sortWithinPartitions($"name", $"time_ms")
      .mapPartitions(partition => {

        // Init the Timeserie processor
        val tsProcessor = new TimeseriesConverter()
        val context = new StandardProcessContext(tsProcessor, "")
        context.setProperty(TimeseriesConverter.GROUPBY.getName, "name")
        context.setProperty(TimeseriesConverter.METRIC.getName,
          s"min;max;avg;trend;outlier;sax:${options.saxAlphabetSize},0.01,${options.saxStringLength}")
        tsProcessor.init(context)

        // Slide over each group
        partition.toList
          .groupBy(row => row.getString(row.fieldIndex("name")))
          .flatMap(group => group._2
            .sliding(options.chunkSize, options.chunkSize)
            .map(subGroup => tsProcessor.toTimeseriesRecord(group._1, chunkStructure, subGroup.toList))
            .map(ts => (ts.getId, tsProcessor.serialize(ts)))
          )
          .iterator
      }).toDF("key", "value")


    // Write this chunk dataframe to Kafka
    /*
    if (options.useKerberos) {
      tsDF
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", options.brokers.get)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("topic", options.out)
        .save()
    } else {
      tsDF
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", options.brokers.get)
        .option("topic", options.out)
        .save()
    }*/


    println("Write to CSV")

    // Write to csv
    tsDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("encoding", "UTF-8")
      .save(options.out)

    tsDF.show();

    println("END OF PROCESSING : ",  format.format(Calendar.getInstance().getTime()))

  }


  def chunkByFile(options: LoaderOptions, spark: SparkSession): Unit = {

    import spark.implicits._


    val pattern = "(.+):((.+)\\/year=(.+)\\/month=(.+)\\/code_install=(.+)\\/name=(.+)\\/(.+))".r
    val tsDF = spark.sparkContext.wholeTextFiles(options.in).map(r => {


      try {
        val filePath = r._1
        val pathTokens = pattern.findAllIn(filePath).matchData.toList.head

        logger.info(s"processing $filePath")
        //  .foreach {  m => println(m.group(4) + " " + m.group(5) + " " + m.group(6) + " " + m.group(7) + " " + m.group(8))     }

        val year = pathTokens.group(4).toInt
        val month = pathTokens.group(5).toInt
        val codeInstall = pathTokens.group(6)
        val name = pathTokens.group(7)

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

              Some(EvoaMeasure(name, codeInstall, sensor, value, quality, timestamp, timeMs, year, month, day, filePath))
            } catch {
              case _: Throwable => None
            }

          })
          .filter(_.isDefined)
          .map(_.get)
          .sortBy(_.timeMs)

        Some((name, measures))
      } catch {
        case _: Throwable => None
      }


    })
      .filter(_.isDefined)
      .map(_.get)
      .flatMap(m => {

        val name = m._1
        val measures = m._2

        // Init the Timeserie processor
        val tsProcessor = new TimeseriesConverter()
        val context = new StandardProcessContext(tsProcessor, "")
        context.setProperty(TimeseriesConverter.GROUPBY.getName, "name")
        context.setProperty(TimeseriesConverter.METRIC.getName,
          s"first;sum;min;max;avg;trend;outlier;sax:${options.saxAlphabetSize},0.005,${options.saxStringLength}")
        tsProcessor.init(context)

        // Slide over each group
        measures.sliding(options.chunkSize, options.chunkSize)
          .map(subGroup => tsProcessor.fromMeasurestoTimeseriesRecord(subGroup.toList))
          .map(ts => (ts.getId, tsProcessor.serialize(ts)))
      })
      .toDF("key", "value")


    // Write this chunk dataframe to Kafka
    if (options.useKerberos) {
      tsDF
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", options.brokers.get)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("topic", options.out)
        .save()
    } else {
      tsDF
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", options.brokers.get)
        .option("topic", options.out)
        .save()
    }

  }


  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {


    val line = "file:/Users/tom/Documents/workspace/ifpen/data-historian/data/preload/year=2019/month=6/code_install=067_PI01/name=067_PI01/part-00000-55cad8b2-8f82-42be-ad77-2872a9082f53.c000.csv"

    val pattern = "^(.*):(.*)$".r


    val tokens = pattern.findAllMatchIn(line).toList

    pattern.findAllIn(line).matchData foreach { m => println(m.group(1)) }

    // get arguments
    val options = parseCommandLine(args)

    // setup spark session
    val spark = SparkSession.builder
      .appName(options.appName)
      //.master(options.master)
      .master("local[*]")
      .getOrCreate()

    // process
    options.mode match {
      case LoaderMode.PRELOAD => preload(options, spark)
      case LoaderMode.CHUNK => chunk(options, spark)
      case LoaderMode.CHUNK_BY_FILE => chunkByFile(options, spark)
      case _ => println(s"unknown loader mode : $LoaderMode")
    }
  }

}
