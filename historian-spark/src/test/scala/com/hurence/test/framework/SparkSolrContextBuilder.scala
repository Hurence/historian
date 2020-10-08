package com.hurence.test.framework

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSolrContextBuilder extends BeforeAndAfterAll { this: Suite =>

  @transient var sparkSession: SparkSession = _
  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .appName("spark-solr-tester")
      .master("local")
      .config("spark.ui.enabled","false")
      .config("spark.default.parallelism", "1")
      .getOrCreate()

    sc = sparkSession.sparkContext
  }

  override def afterAll(): Unit = {
    try {
      sparkSession.stop()
    } finally {
      SparkSession.clearDefaultSession()
      SparkSession.clearActiveSession()
    }
    super.afterAll()
  }
}
