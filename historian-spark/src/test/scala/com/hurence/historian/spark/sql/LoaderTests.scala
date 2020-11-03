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
    assertEquals("adbbbbabbcccbbaeceefgfffffgffffeeeeeeeeedebcbcbbcc", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)


    //count=288, first=1496.6, min=68.8, max=2145.6, sum=475649.5999999999, avg=1651.5611111111111, last=1571.8, stdDev=178.49958904923906, qualityFirst=1497.6, qualityMin=69.8, qualityMax=2146.6, qualitySum=475937.6, qualityAvg=1652.5612, year=2019, month=11, day=2019-11-29, origin=injector, sax=adbbbbabbcccbbaeceefgfffffgffffeeeeeeeeedebcbcbbcc, trend=true, outlier=false, id=d18aeefd124b57c1c9071b898688889426d911481ba70e3b6eaaaff4cb772ba7, metricKey=ack|crit$null|max$null|metric_id$08f9583b-6999-4835-af7d-cf2f82ddcd5d|min$null|warn$null, tags={warn=null, crit=null, min=null, max=null, metric_id=08f9583b-6999-4835-af7d-cf2f82ddcd5d})

    assertEquals(288, chunktoCheck.getCount)
    assertEquals(1651.5611111111111, chunktoCheck.getAvg)
    assertEquals(178.49958904923906, chunktoCheck.getStdDev)
    assertEquals(68.8, chunktoCheck.getMin)
    assertEquals(2145.6, chunktoCheck.getMax)
    assertEquals(1496.6, chunktoCheck.getFirst)

    // qualityFirst=1497.6, qualityMin=69.8, qualityMax=2146.6, qualitySum=475937.6, qualityAvg=1652.5612
    assertEquals(1571.8, chunktoCheck.getLast)
    assertEquals(475649.5999999999, chunktoCheck.getSum)
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

  //@Test
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
      .filter(r => r.getName.equals("068_PI01") || r.getName.equals("455_PI01")).cache()

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
