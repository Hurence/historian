package com.hurence.historian.spark.compactor

import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import profig._

case class SparkConf(master: String = "local[*]",
                     appName: String = "Historian-Compactor",
                     useKerberos: Boolean = false,
                     deployMode: String = "",
                     driverMemory: String = "1g",
                     driverCores: Int = 1,
                     numExecutors: Int = 5,
                     executorMemory: String = "1g",
                     executorCores: Int = 1,
                     sqlShufflePartitions: Int = 10,
                     checkpointDir: String = "checkpoints/historian-compactor")

case class SolrConf(zkHosts: String = "localhost:9983",
                    collectionName: String = "historian",
                    batchSize: Int = 2000,
                    numConcurrentRequests: Int = 2,
                    flushInterval: Int = 2000)

case class ReaderConf(queryFilters: String = "",
                      tagNames: String = "tagname,metric_id")

case class ChunkyfierConf(saxAlphabetSize: Int = 7,
                          saxStringLength: Int = 24,
                          groupByCols: String = "name",
                          origin: String = "compactor",
                          dateBucketFormat: String = "yyyy-MM-dd.HH")

case class SchedulerConf(period: Int = 10,
                         startNow: Boolean = true)

case class CompactorConf(spark: SparkConf,
                         solr: SolrConf,
                         reader: ReaderConf,
                         chunkyfier: ChunkyfierConf,
                         scheduler: SchedulerConf)

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
    try {
      Some(StructType(conf.split(",")
        .map(column => StructField(column, inferType(column), true))))
    } catch {
      case _: Throwable => None
    }

  }

  def defaults() = {
    CompactorConf(SparkConf(), SolrConf(), ReaderConf(), ChunkyfierConf(), SchedulerConf())
  }

  def loadFromFile(filePath: String) = {
    Profig.load(ProfigLookupPath(filePath, FileType.Yaml, LoadType.Merge))
    Profig("conf").as[CompactorConf]
  }
}