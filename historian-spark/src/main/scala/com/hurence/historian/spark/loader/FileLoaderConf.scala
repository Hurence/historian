package com.hurence.historian.spark.loader

import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import profig._

case class SparkConf(master: String = "local[*]",
                     appName: String = "Historian-FileLoader",
                     useKerberos: Boolean = false,
                     deployMode: String = "",
                     driverMemory: String = "1g",
                     driverCores: Int = 1,
                     numExecutors: Int = 5,
                     executorMemory: String = "1g",
                     executorCores: Int = 1,
                     streamingEnabled: Boolean = false,
                     sqlShufflePartitions: Int = 10,
                     checkpointDir: String = "checkpoints/historian-file-loader")

case class SolrConf(zkHosts: String = "localhost:2181",
                    collectionName: String = "historian",
                    batchSize: Int = 2000,
                    numConcurrentRequests: Int = 2,
                    flushInterval: Int = 2000)

/**
  * Maximum age of a file that can be found in this directory, before it is ignored. For the
  * first batch all files will be considered valid. If `latestFirst` is set to `true` and
  * `maxFilesPerTrigger` is set, then this parameter will be ignored, because old files that are
  * valid, and should be processed, may be ignored. Please refer to SPARK-19813 for details.
  *
  * The max age is specified with respect to the timestamp of the latest file, and not the
  * timestamp of the current system. That this means if the last file has timestamp 1000, and the
  * current system time is 2000, and max age is 200, the system will purge files older than
  * 800 (rather than 1800) from the internal state.
  *
  * Default to a week.
  */
case class CSVReaderConf(csvFilePath: String = ".",
                         maxFileAge: String = "1h",
                         maxFilesPerTrigger: Int = -1,
                         encoding: String = "UTF-8",
                         schema: String = "",
                         tagNames: String = "",
                         groupByCols: String = "name",
                         timestampField: String = "timestamp",
                         nameField: String = "name",
                         valueField: String = "value",
                         qualityField: String = "quality",
                         timestampFormat: String = "dd/MM/yyyy HH:mm:ss",
                         columnDelimiter: String = ",")

case class ChunkyfierConf(chunkSize: Int = 1440,
                          saxAlphabetSize: Int = 7,
                          saxStringLength: Int = 24,
                          groupByCols: String = "name",
                          origin: String = "file-loader",
                          dateBucketFormat: String = "yyyy-MM-dd.HH",
                          watermarkDelayThreshold: String = "5 minutes",
                          windowDuration: String = "2 minutes")

case class FileLoaderConf(spark: SparkConf, solr: SolrConf, reader: CSVReaderConf, chunkyfier: ChunkyfierConf)

object ConfigLoader {

  def inferType(field: String) = {
      field.split(":")(1).toLowerCase match {
        case "int" => IntegerType
        case "integer" => IntegerType
        case "long" => LongType
        case "double" => DoubleType
        case "float" => FloatType
        case "bool" => BooleanType
        case "boolean" => BooleanType
        case "string" => StringType
        case _ => StringType
      }
  }

  def toSchema(conf: String) = {
    try{
      Some(StructType(conf.split(",")
        .map(column => StructField(column, inferType(column), true))))
    }catch {
      case _:Throwable => None
    }

  }

  def defaults() = {
    FileLoaderConf(SparkConf(), SolrConf(), CSVReaderConf(), ChunkyfierConf())
  }

  def loadFromFile(filePath: String) = {
    Profig.load(ProfigLookupPath(filePath, FileType.Yaml, LoadType.Merge))
    Profig("conf").as[FileLoaderConf]
  }
}