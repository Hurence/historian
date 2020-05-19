package com.hurence.test.framework

import java.util.UUID

import com.hurence.historian.spark.solr.SolrCloudUtil
import com.lucidworks.spark.example.ml.DateConverter
import com.lucidworks.spark.util.SolrSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

trait MovielensBuilder extends SparkSolrTests with BeforeAndAfterAll with BeforeAndAfterEach {

  val uuid = UUID.randomUUID().toString.replace("-", "_")
  val moviesColName: String = s"movielens_movies_$uuid"
  val ratingsColName: String = s"movielens_ratings_$uuid"
  val userColName: String = s"movielens_users_$uuid"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createCollections()
    MovielensBuilder.indexMovieLensDataset(sparkSession, zkHost, uuid)
    SolrSupport.getCachedCloudClient(zkHost).commit(moviesColName)
    SolrSupport.getCachedCloudClient(zkHost).commit(ratingsColName)
    val opts = Map(
      "zkhost" -> zkHost,
      "collection" -> moviesColName)
    val df = sparkSession.read.format("solr").options(opts).load()
    df.createOrReplaceTempView(moviesColName)
  }

  override def afterAll(): Unit = {
    deleteCollections()
    super.afterAll()
  }

  def createCollections(): Unit = {
    SolrCloudUtil.buildCollection(zkHost, moviesColName, null, 1, cloudClient, sc)
    SolrCloudUtil.buildCollection(zkHost, ratingsColName, null, 1, cloudClient, sc)
//    SolrCloudUtil.buildCollection(zkHost, userColName, null, 1, cloudClient, sc)
  }

  def deleteCollections(): Unit = {
    SolrCloudUtil.deleteCollection(ratingsColName, cluster)
    SolrCloudUtil.deleteCollection(moviesColName, cluster)
//    SolrCloudUtil.deleteCollection(userColName, cluster)
  }
}

object MovielensBuilder {
  val dataDir: String = "src/test/resources/ml-100k"

  def indexMovieLensDataset(sparkSession: SparkSession, zkhost: String, uuid: String): Unit = {
    //    val userDF = sqlContext.read.json(dataDir + "/movielens_users.json")
    //    userDF.write.format("solr").options(Map("zkhost" -> zkhost, "collection" -> "movielens_users", "batch_size" -> "10000")).save

    val moviesDF = sparkSession.read.json(dataDir + "/movielens_movies.json")
    moviesDF.write.format("solr").options(Map("zkhost" -> zkhost, "collection" -> s"movielens_movies_$uuid", "batch_size" -> "10000")).save

    val ratingsDF = sparkSession.read.json(dataDir + "/movielens_ratings_10k.json")
    val dateUDF = udf(DateConverter.toISO8601(_: String))
    ratingsDF
      .withColumn("timestamp", dateUDF(ratingsDF("rating_timestamp")))
      .drop("rating_timestamp")
      .withColumnRenamed("timestamp", "rating_timestamp")
      .limit(10000)
      .write
      .format("solr")
      .options(Map("zkhost" -> zkhost, "collection" -> s"movielens_ratings_$uuid", "batch_size" -> "10000"))
      .save
  }
}


