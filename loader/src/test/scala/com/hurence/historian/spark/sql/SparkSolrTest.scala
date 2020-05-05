package com.hurence.historian.spark.sql

import java.util.UUID

import org.apache.spark.sql.functions.{base64,col}
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.solr.{SolrCloudUtil, TestSuiteBuilder}
import com.lucidworks.spark.util.SolrSupport
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class SparkSolrTest extends TestSuiteBuilder {

  test("Solr version") {
    val solrVersion = SolrSupport.getSolrVersion(zkHost)
    assert(solrVersion == "8.2.0")
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 7, 5, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 7, 3, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 7, 1, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 8, 0, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 8, 1, 0))
    assert(!SolrSupport.isSolrVersionAtleast(solrVersion, 9, 0, 0))
  }



  test("measures and chunks") {
    val collectionName = "testHistorian-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)
    try {


      val chunkyfier = new Chunkyfier()
        .setValueCol("value")
        .setTimestampCol("timestamp")
        .setChunkCol("chunk")
        .setGroupByCols(Array("name", "tags.metric_id"))
        .setDateBucketFormat("yyyy-MM-dd")
        .doDropLists(false)
        .setSaxAlphabetSize(7)
        .setSaxStringLength(50)


      val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath


      val measures = sparkSession.read
        .parquet(filePath)



      // TODO add Transformer that generates an id for solr
      // Transform original data into its bucket index.
      val ack08 = chunkyfier.transform(measures)
        .where("name = 'ack' AND avg != 0.0")
        .withColumn("id", base64(col("chunk")))
        .repartition(1)


      val solrOpts = Map("zkhost" -> zkHost, "collection" -> collectionName)
      ack08.write.format("solr").options(solrOpts).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collectionName, true, true)

      val solrDF = sparkSession.read.format("solr").options(solrOpts).load()

      solrDF.show()
      assert(solrDF.count == 70)
    /*  assert(solrDF.schema.fields.length === 5) // _root_ id one_txt two_txt three_s
      val oneColFirstRow = solrDF.select("one_txt").head()(0) // query for one column
      assert(oneColFirstRow != null)
      val firstRow = solrDF.head.toSeq                        // query for all columns
      assert(firstRow.size === 5)
      firstRow.foreach(col => assert(col != null))            // no missing values*/

    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }


  test("vary queried columns") {
    val collectionName = "testQuerying-" + UUID.randomUUID().toString
    SolrCloudUtil.buildCollection(zkHost, collectionName, null, 1, cloudClient, sc)
    try {
      val csvDF = buildTestData()
      val solrOpts = Map("zkhost" -> zkHost, "collection" -> collectionName)
      csvDF.write.format("solr").options(solrOpts).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)
      solrCloudClient.commit(collectionName, true, true)

      val solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 3)
      assert(solrDF.schema.fields.length === 5) // _root_ id one_txt two_txt three_s
      val oneColFirstRow = solrDF.select("one_txt").head()(0) // query for one column
      assert(oneColFirstRow != null)
      val firstRow = solrDF.head.toSeq                        // query for all columns
      assert(firstRow.size === 5)
      firstRow.foreach(col => assert(col != null))            // no missing values

    } finally {
      SolrCloudUtil.deleteCollection(collectionName, cluster)
    }
  }

  def buildTestData() : DataFrame = {
    val testDataSchema : StructType = StructType(
      StructField("id", IntegerType, true) ::
        StructField("one_txt", StringType, false) ::
        StructField("two_txt", StringType, false) ::
        StructField("three_s", StringType, false) :: Nil)

    val rows = Seq(
      Row(1, "A", "B", "C"),
      Row(2, "C", "D", "E"),
      Row(3, "F", "G", "H")
    )

    val csvDF : DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.makeRDD(rows, 1), testDataSchema)
    assert(csvDF.count == 3)
    return csvDF
  }

}