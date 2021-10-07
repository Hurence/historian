package com.hurence.historian.spark.sql

import java.util.UUID

import com.hurence.historian.spark.ml.{Chunkyfier, UnChunkyfier}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader.{ReaderFactory, ReaderType}
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.hurence.historian.{SolrCloudUtilForTests, SolrUtils}
import com.hurence.test.framework.SparkSolrTests
import com.hurence.timeseries.model.{Chunk, Measure}
import com.lucidworks.spark.util.SolrSupport
import io.vertx.core.json.JsonArray
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.hurence.historian.spark.sql.functions.reorderColumns
import com.hurence.timeseries.model.Definitions.FIELD_QUALITY_AVG
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.functions.col
import org.junit.Assert.{assertArrayEquals, assertEquals}

import scala.Seq
import scala.collection.JavaConverters._

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
    val collectionName = "testHistorian-" + UUID.randomUUID().toString
    SolrCloudUtilForTests.buildChunkCollection(collectionName, null, 1, cloudClient)
    try {
      // 1. load measures from parquet file

      val measures = ReaderFactory.getMeasuresReader(ReaderType.PARQUET)
        .read(Options(this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath, Map()))
        .as[Measure](Encoders.bean(classOf[Measure]))
        .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d'")
        .cache()

      if (logger.isDebugEnabled) {
        measures.show()
      }

      // 2. make chunks from measures
      val chunkyfier = new Chunkyfier()
        .setDateBucketFormat("yyyy-MM-dd")
        .setSaxAlphabetSize(7)
        .setSaxStringLength(50)

      val ack08 = chunkyfier.transform(measures)
        .repartition(1)
        .as[Chunk](Encoders.bean(classOf[Chunk]))

      if (logger.isDebugEnabled) {
        ack08.show()
      }

      // 3. write those chunks to SolR
      val writer = WriterFactory.getChunksWriter(WriterType.SOLR)
      writer.write(sql.Options(collectionName, Map(
        "zkhost" -> zkAddressSolr,
        "collection" -> collectionName,
        "tag_names" -> "metric_id,min,max,warn,crit"
      )), ack08)


      // 4. Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkAddressSolr)
      solrCloudClient.commit(collectionName, true, true)

      val q = new SolrQuery("*:*")
      val response = solrCloudClient.query(collectionName, q)

      // 5. load back those chunks to verify
      val reader = ReaderFactory.getChunksReader(ReaderType.SOLR)
      val solrDF = reader.read(sql.Options(collectionName, Map(
        "zkhost" -> zkAddressSolr,
        "collection" -> collectionName,
        "tag_names" -> "metric_id,min,max,warn,crit"
      )))
        .as[Chunk](Encoders.bean(classOf[Chunk]))
      if (logger.isDebugEnabled) {
        solrDF.show()
      }


      val unchunkyfier = new UnChunkyfier()
      val measuresBack = unchunkyfier.transform(solrDF)
        .as[Measure](Encoders.bean(classOf[Measure]))

      if (logger.isDebugEnabled) {
        measuresBack.show()
      }

      assertEquals(
        measures.sort("timestamp").collect().toList.asJava,
        measuresBack.sort("timestamp").collect().toList.asJava
      )


      assert(solrDF.count == 6)
      /*  assert(solrDF.schema.fields.lengthTestda === 5) // _root_ id one_txt two_txt three_s
        val oneColFirstRow = solrDF.select("one_txt").head()(0) // query for one column
        assert(oneColFirstRow != null)
        val firstRow = solrDF.head.toSeq                        // query for all columns
        assert(firstRow.size === 5)
        firstRow.foreach(col => assert(col != null))            // no missing values*/
      SolrCloudUtilForTests.dumpSolrCollection(collectionName, 500, cloudClient)
    } finally {
      SolrCloudUtilForTests.deleteCollection(collectionName, cloudClient)
    }
  }

  test("vary queried columns") {
    val collectionName = "testQuerying-" + UUID.randomUUID().toString
    val solrUrl = "http://" + zkHost
    SolrCloudUtilForTests.buildChunkCollection(collectionName, null, 1, cloudClient)
    try {
      val csvDF = buildTestData()
      val solrOpts = Map("zkhost" -> zkAddressSolr, "collection" -> collectionName)
      csvDF.write.format("solr").options(solrOpts).mode(Overwrite).save()

      // Explicit commit to make sure all docs are visible
      val solrCloudClient = SolrSupport.getCachedCloudClient(zkAddressSolr)
      solrCloudClient.commit(collectionName, true, true)

      val solrDF = sparkSession.read.format("solr").options(solrOpts).load()
      assert(solrDF.count == 3)
      assert(solrDF.schema.fields.length === 4)
      val oneColFirstRow = solrDF.select("name").head()(0) // query for one column
      assert(oneColFirstRow != null)
      val firstRow = solrDF.head.toSeq // query for all columns
      assert(firstRow.size === 4)
      firstRow.foreach(col => assert(col != null)) // no missing values

    } finally {
      SolrCloudUtilForTests.deleteCollection(collectionName, cloudClient)
    }
  }

  def buildTestData(): DataFrame = {
    val testDataSchema: StructType = StructType(
      StructField("id", IntegerType, true) ::
        StructField("name", StringType, false) ::
        StructField("code_install", StringType, false) ::
        StructField("sensor", StringType, false) :: Nil)

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