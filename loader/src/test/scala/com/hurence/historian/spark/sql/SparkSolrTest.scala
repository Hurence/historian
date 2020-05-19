package com.hurence.historian.spark.sql

import java.util.UUID

import com.hurence.historian.SolrCloudUtilForTests
import com.hurence.historian.model.ChunkRecordV0
import com.hurence.historian.modele.SchemaVersion
import com.hurence.historian.solr.util.SolrITHelper
import com.hurence.historian.spark.compactor.job.ChunkModele
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, ReaderFactory}
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.hurence.test.framework.SparkSolrTests
import com.lucidworks.spark.util.SolrSupport
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class SparkSolrTest extends SparkSolrTests {

  test("Solr version") {
    val solrVersion = SolrSupport.getSolrVersion(zkAddressSolr)
    assert(solrVersion == "8.2.0")
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 7, 5, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 7, 3, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 7, 1, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 8, 0, 0))
    assert(SolrSupport.isSolrVersionAtleast(solrVersion, 8, 1, 0))
    assert(!SolrSupport.isSolrVersionAtleast(solrVersion, 9, 0, 0))
  }

  test("Measures and chunks") {

    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    val collectionName = "testHistorian-" + UUID.randomUUID().toString
    SolrCloudUtilForTests.buildCollection(collectionName, null, 1, cloudClient)
    try {
      // 1. load measures from parquet file
      val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath
      val measures = sparkSession.read
        .parquet(filePath)
        .cache()

      // 2. make chunks from measures
      val chunkyfier = new Chunkyfier()
        .setValueCol("value")
        .setTimestampCol("timestamp")
        .setChunkCol("chunk")
        .setGroupByCols(Array("name", "tags.metric_id"))
        .setDateBucketFormat("yyyy-MM-dd")
        .setSaxAlphabetSize(7)
        .setSaxStringLength(50)
      val ack08 = chunkyfier.transform(measures)
        .where("name = 'ack' AND avg != 0.0")
        .repartition(1)
        .as[ChunkRecordV0]

      ack08.show()

      // 3. write those chunks to SolR
      val solrOpts = Map("zkhost" -> zkAddressSolr, "collection" -> collectionName)


      val writer = WriterFactory.getChunksWriter(WriterType.SOLR)
      writer.write(sql.Options(collectionName, Map(
        "zkhost" -> zkAddressSolr,
        "collection" -> collectionName,
        "tag_names" -> "metric_id,min,max,warn,crit"
      )), ack08)


      // 4. Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkAddressSolr)
      solrCloudClient.commit(collectionName, true, true)


      // 5. load back those chunks to verify
      val reader = ReaderFactory.getChunksReader(ChunksReaderType.SOLR)
      val solrDF = reader.read(sql.Options(collectionName, Map(
        "zkhost" -> zkAddressSolr,
        "collection" -> collectionName,
        "tag_names" -> "metric_id"
      )))
        .where("metric_id LIKE '08%'")
      solrDF.show()
      assert(solrDF.count == 5)
      /*  assert(solrDF.schema.fields.length === 5) // _root_ id one_txt two_txt three_s
        val oneColFirstRow = solrDF.select("one_txt").head()(0) // query for one column
        assert(oneColFirstRow != null)
        val firstRow = solrDF.head.toSeq                        // query for all columns
        assert(firstRow.size === 5)
        firstRow.foreach(col => assert(col != null))            // no missing values*/

    } finally {
      SolrCloudUtilForTests.deleteCollection(collectionName, cloudClient)
    }
  }


  test("vary queried columns") {
    val collectionName = "testQuerying-" + UUID.randomUUID().toString
    val solrUrl = "http://" + zkHost
    SolrCloudUtilForTests.buildCollection(collectionName, null, 1, cloudClient)
    try {
      val csvDF = buildTestData()
      val solrOpts = Map("zkhost" -> zkAddressSolr, "collection" -> collectionName)
      csvDF.write.format("solr").options(solrOpts).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkAddressSolr)
      solrCloudClient.commit(collectionName, true, true)

      val solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 3)
      assert(solrDF.schema.fields.length === 5) // _root_ id one_txt two_txt three_s
      val oneColFirstRow = solrDF.select("one_txt").head()(0) // query for one column
      assert(oneColFirstRow != null)
      val firstRow = solrDF.head.toSeq // query for all columns
      assert(firstRow.size === 5)
      firstRow.foreach(col => assert(col != null)) // no missing values

    } finally {
      SolrCloudUtilForTests.deleteCollection(collectionName, cloudClient)
    }
  }

  def buildTestData(): DataFrame = {
    val testDataSchema: StructType = StructType(
      StructField("id", IntegerType, true) ::
        StructField("one_txt", StringType, false) ::
        StructField("two_txt", StringType, false) ::
        StructField("three_s", StringType, false) :: Nil)

    val rows = Seq(
      Row(1, "A", "B", "C"),
      Row(2, "C", "D", "E"),
      Row(3, "F", "G", "H")
    )

    val csvDF: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.makeRDD(rows, 1), testDataSchema)
    assert(csvDF.count == 3)
    return csvDF
  }

}