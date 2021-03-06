package com.hurence.historian.spark.ml

import java.util

import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.historian.spark.sql.Options
import com.hurence.historian.spark.sql.functions.toDateUTC
import com.hurence.historian.spark.sql.reader.{ReaderFactory, ReaderType}
import com.hurence.timeseries.model.Chunk.MetricKey.{TAG_KEY_VALUE_SEPARATOR_CHAR, TOKEN_SEPARATOR_CHAR}
import com.hurence.timeseries.model.{Chunk, Measure}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{avg, count, lit, max, min, _}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

@TestInstance(Lifecycle.PER_CLASS)
class ChunkyfierTests extends SparkSessionTestWrapper {

  private val logger = LoggerFactory.getLogger(classOf[ChunkyfierTests])

  @BeforeAll
  def init(): Unit = {
    // to lazy load spark if needed
    spark.version
  }

  @Test
  def testChunkyfier() = {

    val chunkyfier = new Chunkyfier()
      .setGroupByCols(Array("name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .doDropLists(false)
      .setSaxAlphabetSize(7)
      .setSaxStringLength(50)

    if (logger.isDebugEnabled) {
      it4MetricsDS.show()
    }

    // Transform original data into its bucket index.
    val sample = chunkyfier.transform(
      it4MetricsDS.where("name = 'ack' AND " +
        "tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND " +
        "day = '2019-11-29'")
    ).as[Chunk](Encoders.bean(classOf[Chunk]))
      .collect()

    checkChunk(sample(0))
  }

  @Test
  def testChunkyfierNoTags() = {

    val chunkyfier = new Chunkyfier()
      .setGroupByCols(Array("name"))
      .setDateBucketFormat("yyyy-MM-dd")
      .doDropLists(false)
      .setSaxAlphabetSize(7)
      .setSaxStringLength(50)

    val it4MetricsDSNoTags = it4MetricsDS
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .drop("tags")

    if (logger.isDebugEnabled) {
      it4MetricsDSNoTags.show()
      it4MetricsDSNoTags.printSchema()
    }

    // Transform original data into its bucket index.
    val sample = chunkyfier.transform(it4MetricsDSNoTags)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .collect()

    checkChunkNoTags(sample(0));
  }

  def checkChunkNoTags(chunktoCheck: Chunk) = {

    assertEquals("a2c2bb92ab5e8a233ee495c8a0d860a33f2d128dcdd3f673641ba36d291a44b3", chunktoCheck.getId);
    assertEquals("ack", chunktoCheck.getMetricKey);

    assertEquals(1574985682000L, chunktoCheck.getStart)
    assertEquals(1575071765000L, chunktoCheck.getEnd)
    assertEquals("ack", chunktoCheck.getName)
    assertEquals("abbbabbcbbbaadceffgfggfggfgffffeeeffeedebcbcabccba", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)


    assertEquals(288, chunktoCheck.getCount)
    assertEquals(1652.8368055555557, chunktoCheck.getAvg)
    assertEquals(150.6970569095115, chunktoCheck.getStdDev)
    assertEquals(1085.4, chunktoCheck.getMin)
    assertEquals(2045.6, chunktoCheck.getMax)
    assertEquals(1597.8, chunktoCheck.getFirst)
    assertEquals(1463.0, chunktoCheck.getLast)
  }

  def checkChunk(chunktoCheck: Chunk) = {

    //  assertEquals("bda0dab70729290db3183474594d4e459afce8b89693132bb819d81309651a3d", chunktoCheck.getId);
    assertEquals("ack" + TOKEN_SEPARATOR_CHAR +
      "crit" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null" + TOKEN_SEPARATOR_CHAR +
      "max" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null" + TOKEN_SEPARATOR_CHAR +
      "metric_id" + TAG_KEY_VALUE_SEPARATOR_CHAR + "08f9583b-6999-4835-af7d-cf2f82ddcd5d" + TOKEN_SEPARATOR_CHAR +
      "min" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null" + TOKEN_SEPARATOR_CHAR +
      "warn" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null", chunktoCheck.getMetricKey);

    assertEquals(1574985682000L, chunktoCheck.getStart)
    assertEquals(1575071765000L, chunktoCheck.getEnd)
    assertEquals("ack", chunktoCheck.getName)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", chunktoCheck.getTag("metric_id"))
    assertEquals("abbbabbcbbbaadceffgfggfggfgffffeeeffeedebcbcabccba", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)


    assertEquals(288, chunktoCheck.getCount)
    assertEquals(1652.8368055555557, chunktoCheck.getAvg)
    assertEquals(150.6970569095115, chunktoCheck.getStdDev)
    assertEquals(1085.4, chunktoCheck.getMin)
    assertEquals(2045.6, chunktoCheck.getMax)
    assertEquals(1597.8, chunktoCheck.getFirst)

    // qualityFirst=1497.6, qualityMin=69.8, qualityMax=2146.6, qualitySum=475937.6, qualityAvg=1652.5612
    assertEquals(1463.0, chunktoCheck.getLast)
    assertEquals(476017.0, chunktoCheck.getSum)
    assertFalse(chunktoCheck.isOutlier);
    assertTrue(chunktoCheck.isTrend)
    assertEquals(2019, chunktoCheck.getYear)
    assertEquals(11, chunktoCheck.getMonth)
    assertEquals("2019-11-29", chunktoCheck.getDay)

    /*  assertEquals(1497.5999755859375, chunktoCheck.getQualityFirst)
    assertEquals(69.80000305175781, chunktoCheck.getQualityMin)
    assertEquals(2146.60009765625, chunktoCheck.getQualityMax)
     assertEs(475937.6, chunktoCheck.getQualitySum)
    assertEquals(1652.5612, chunktoCheck.getQualityAvg)*/
  }


  import spark.implicits._

  @Test
  def testRandomMeasuresProcessing() = {


    // build a bunch of random measures
    val name = "metric"
    val tags = new util.HashMap[String, String]() {}
    val inputMeasures = ListBuffer[Measure]()
    for (i <- 0 until 1000) {
      inputMeasures += (randomMeasure(name, tags, 0, 100, "1977-03-02"))
    }

    val ds = spark.sparkContext.parallelize(inputMeasures).toDS()
    // convert them as a Chunk


    val unchunkyfier = new UnChunkyfier()
    val chunkyfier = new Chunkyfier()
      .setOrigin("chunkyfierTest")
      .setGroupByCols("name".split(","))
      .setDateBucketFormat("yyyy-MM-dd.HH")
      .setSaxAlphabetSize(7)
      .setSaxStringLength(24)


    val transformedDS = unchunkyfier.transform(
      chunkyfier.transform(ds)
    ).as[Measure]


    org.junit.Assert.assertEquals("The Datasets differs",
      ds.sort("timestamp").collect().toList.asJava,
      transformedDS.sort("timestamp").collect().toList.asJava
    )

  }

}
