package com.hurence.historian.spark.sql

import com.hurence.historian.spark.SparkSessionTestWrapper
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, MeasuresReaderType, ReaderFactory}
import com.hurence.timeseries.model.Chunk
import com.hurence.timeseries.model.Chunk.MetricKey.TOKEN_SEPARATOR_CHAR
import com.hurence.timeseries.model.Chunk.MetricKey.TAG_KEY_VALUE_SEPARATOR_CHAR;
import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}
import org.slf4j.LoggerFactory

@TestInstance(Lifecycle.PER_CLASS)
class LoaderTests extends SparkSessionTestWrapper {

  private val logger = LoggerFactory.getLogger(classOf[LoaderTests])

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
    val sample = chunkyfier.transform(it4MetricsDS)
      .where("name = 'ack' AND tags.metric_id = '08f9583b-6999-4835-af7d-cf2f82ddcd5d' AND day = '2019-11-29'")
      .as[Chunk](Encoders.bean(classOf[Chunk]))
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


    it4MetricsDSNoTags.show()
    it4MetricsDSNoTags.printSchema()

    // Transform original data into its bucket index.
    val sample = chunkyfier.transform(it4MetricsDSNoTags)
      .as[Chunk](Encoders.bean(classOf[Chunk]))
      .collect()

    checkChunkNoTags(sample(0));
  }

  def checkChunkNoTags(chunktoCheck: Chunk) = {

    assertEquals("17eab44f1115a3fd8ac07ee69b68a70897cede7c11169a39442ed5d74a50e7bc", chunktoCheck.getId);
    assertEquals("ack", chunktoCheck.getMetricKey);

    assertEquals(1574985682000L, chunktoCheck.getStart)
    assertEquals(1575068166000L, chunktoCheck.getEnd)
    assertEquals("ack", chunktoCheck.getName)
    assertEquals("abbbabbbbcbbacecfeggfgffggfgfffefeeffefdedacbbabbc", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)

    assertEquals(276, chunktoCheck.getCount)
    assertEquals(1659.1731884057972, chunktoCheck.getAvg)
    assertEquals(149.96408784159198, chunktoCheck.getStdDev)
    assertEquals(1085.4, chunktoCheck.getMin)
    assertEquals(2045.6, chunktoCheck.getMax)
    assertEquals(1597.8, chunktoCheck.getFirst)
    assertEquals(1615.4, chunktoCheck.getLast)
  }

  def checkChunk(chunktoCheck: Chunk) = {

  //  assertEquals("bda0dab70729290db3183474594d4e459afce8b89693132bb819d81309651a3d", chunktoCheck.getId);
    assertEquals("ack" + TOKEN_SEPARATOR_CHAR +
      "crit" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null" + TOKEN_SEPARATOR_CHAR +
      "max" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null" + TOKEN_SEPARATOR_CHAR +
      "metric_id" + TAG_KEY_VALUE_SEPARATOR_CHAR + "08f9583b-6999-4835-af7d-cf2f82ddcd5d" + TOKEN_SEPARATOR_CHAR +
      "min" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null" + TOKEN_SEPARATOR_CHAR +
      "warn" + TAG_KEY_VALUE_SEPARATOR_CHAR + "null", chunktoCheck.getMetricKey);

    assertEquals(1574982082000L, chunktoCheck.getStart)
    assertEquals(1575068166000L, chunktoCheck.getEnd)
    assertEquals("ack", chunktoCheck.getName)
    assertEquals("08f9583b-6999-4835-af7d-cf2f82ddcd5d", chunktoCheck.getTag("metric_id"))
    assertEquals("acabbbabbcccbbaddeefgffffggfgffefeeeefeedebcbcbbcc", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)

    assertEquals(288, chunktoCheck.getCount)
    assertEquals(1651.5611111111111, chunktoCheck.getAvg)
    assertEquals(178.49958904923906, chunktoCheck.getStdDev)
    assertEquals(68.8, chunktoCheck.getMin)
    assertEquals(2145.6, chunktoCheck.getMax)
    assertEquals(1496.6, chunktoCheck.getFirst)
    assertEquals(1615.4, chunktoCheck.getLast)
    assertEquals(475649.5999999999, chunktoCheck.getSum)
    assertFalse(chunktoCheck.isOutlier);
    assertTrue(chunktoCheck.isTrend)
    assertEquals(2019, chunktoCheck.getYear)
    assertEquals(11, chunktoCheck.getMonth)
    assertEquals("2019-11-29", chunktoCheck.getDay)

    assertEquals(1497.5999755859375, chunktoCheck.getQualityFirst)
    assertEquals(69.80000305175781, chunktoCheck.getQualityMin)
    assertEquals(2146.60009765625, chunktoCheck.getQualityMax)
   /* assertEquals(475937.6, chunktoCheck.getQualitySum)
    assertEquals(1652.5612, chunktoCheck.getQualityAvg)*/
}

  @Test
  def testLoaderCSV() = {

    val reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.csv.gz").getPath
    val options = Options(
      filePath,
      Map(
        "inferSchema" -> "true",
        "delimiter" -> ",",
        "header" -> "true",
        "nameField" -> "metric_name",
        "timestampField" -> "timestamp",
        "qualityField" -> "",
        "timestampDateFormat" -> "s",
        "valueField" -> "value",
        "tagsFields" -> "metric_id,warn,crit"
      ))

    val ds = reader.read(options)

    if (logger.isDebugEnabled) {
      ds.show()
    }

  }

  @Test
  def testLoaderCSV2() = {

    val reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
    val filePath = this.getClass.getClassLoader.getResource("chemistry").getPath
    val options = com.hurence.historian.spark.sql.Options(
      filePath + "/dataHistorian-ISNTS35-N-2020030100*.csv",
      Map(
        "inferSchema" -> "true",
        "delimiter" -> ";",
        "header" -> "true",
        "nameField" -> "tagname",
        "timestampField" -> "timestamp",
        "qualityField" -> "quality",
        "timestampDateFormat" -> "dd/MM/yyyy HH:mm:ss",
        "valueField" -> "value",
        "tagsFields" -> "tagname"
      ))

    val ds = reader.read(options)
      .filter(r => r.getName.equals("068_PI01") || r.getName.equals("455_PI01"))      .cache()

    ds.show()
   System.out.println(ds.count())

    val chunkyfier = new Chunkyfier()
      .setOrigin("chemistry")
      .setGroupByCols("name".split(","))
      .setDateBucketFormat("yyyy-MM-dd")
      .setSaxAlphabetSize(7)
      .setSaxStringLength(24)

    val chunksDS = chunkyfier.transform(ds)
      .as[Chunk](Encoders.bean(classOf[Chunk]))


   // if (logger.isDebugEnabled)
    {


      chunksDS.show()
    }


  }


  @Test
  def testLoadITDataCSVV0() = {

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.csv.gz").getPath
    val options = Options(
      filePath,
      Map(
        "inferSchema" -> "true",
        "delimiter" -> ",",
        "header" -> "true",
        "dateFormat" -> ""
      ))
    val itDataV0Reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.ITDATA_CSV)

    val ds = itDataV0Reader.read(options)

    if (logger.isDebugEnabled) {
      ds.show()
    }
  }

  @Test
  def testLoadITDataParquetV0() = {

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics.parquet").getPath
    val options = Options(filePath, Map())
    val itDataV0Reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.PARQUET)

    val ds = itDataV0Reader.read(options)

    if (logger.isDebugEnabled) {
      ds.printSchema()
      ds.show()
    }

  }


  @Test
  def testLoadITDataChunksParquetV0() = {

    val filePath = this.getClass.getClassLoader.getResource("it-data-4metrics-chunk.parquet").getPath
    val options = Options(filePath, Map())
    val reader = ReaderFactory.getChunksReader(ChunksReaderType.PARQUET)
    val ds = reader.read(options)

    if (logger.isDebugEnabled) {
      ds.printSchema()
      ds.show()
    }

  }
}
