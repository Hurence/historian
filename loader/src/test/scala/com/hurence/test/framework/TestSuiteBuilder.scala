package com.hurence.test.framework

import com.hurence.historian.spark.solr.SparkSolrFunSuite

// General builder to be used by all the tests that need Solr and Spark running
trait TestSuiteBuilder extends SparkSolrFunSuite with SparkSolrContextBuilder with SolrCloudTestBuilder {}
