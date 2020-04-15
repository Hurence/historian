package com.hurence.historian.spark.compactor

import org.apache.spark.sql.SparkSession

trait ChunkCompactor extends Serializable {

  /**
   * Compact chunks of historian
   */
  def run(spark: SparkSession): Unit
}
