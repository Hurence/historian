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

    assertEquals("e40bc0bb0ad390d9ed6c9c13e27559f2ac82ce63dec4d4b95e266a31988719ec", chunktoCheck.getId);
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
    assertEquals("abababbbbbbaaeceefgggffggfgffffeeefefedebcbcabcbba", chunktoCheck.getSax)
    assertEquals("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", chunktoCheck.getValueAsString)


    System.out.println(chunktoCheck.toString)

    assertEquals(288, chunktoCheck.getCount)
    assertEquals(1652.8368055555557, chunktoCheck.getAvg)
    assertEquals(150.6970569095115, chunktoCheck.getStdDev)
    assertEquals(1085.4, chunktoCheck.getMin)
    assertEquals(2045.6, chunktoCheck.getMax)
    assertEquals(1597.8, chunktoCheck.getFirst)

    // qualityFirst=1497.6, qualityMin=69.8, qualityMax=2146.6, qualitySum=475937.6, qualityAvg=1652.5612
    assertEquals(1489.8, chunktoCheck.getLast)
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
