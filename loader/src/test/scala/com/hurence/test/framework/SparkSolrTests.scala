package com.hurence.test.framework

// General builder to be used by all the tests that need Solr and Spark running
trait SparkSolrTests extends SparkSolrFunSuite with SparkSolrContextBuilder with SolrCloudTestBuilder {}
