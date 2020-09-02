git statuspackage com.hurence.historian.spark.sql.transformer

import java.util.Date

import com.hurence.historian.modele.ChunkRecordV0
import com.hurence.historian.spark.ml.Chunkyfier
import org.apache.spark.sql.SparkSession


object RechunkifyerExemple {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ReChunkyfierExample")
      .getOrCreate()
    import spark.implicits._

    val now = new Date().getTime
    val oneSec = 1000L
    val oneMin = 60 * oneSec
    val oneHour = 60 * oneMin
    val oneDay = 24 * oneHour


    val dataFrame = spark.createDataFrame(Seq(
      ("metric_a", 1.3, now + 30 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.2, now + 20 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.1, now + 10 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.4, now + 40 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.5, now + 50 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.6, now + oneHour, Map("metric_id" -> 1)),
      ("metric_a", 1.7, now + oneDay + 10 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 1.8, now + oneDay + 20 * oneMin, Map("metric_id" -> 1)),
      ("metric_a", 2.1, now + 10 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.2, now + 20 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.3, now + 30 * oneMin, Map("metric_id" -> 2)),
      ("metric_a", 2.4, now + oneHour, Map("metric_id" -> 2)),
      ("metric_b", 3.1, now + 10 * oneMin, Map("metric_id" -> 3)),
      ("metric_b", 3.2, now + 20 * oneMin, Map("metric_id" -> 3))
    ))
      .toDF("name", "value", "timestamp", "tags")

    val chunkyfier = new Chunkyfier()
      .setValueCol("value")
      .setTimestampCol("timestamp")
      .setChunkCol("chunk")
      .setGroupByCols(Array("name", "tags.metric_id"))
      .setDateBucketFormat("yyyy-MM-dd")
      .setChunkMaxSize(1440)
      .setSaxAlphabetSize(4)
      .setSaxStringLength(4)

    val bucketedData = chunkyfier.transform(dataFrame).as[ChunkRecordV0]
    bucketedData.show()


//    val options = new RechunkifyerOptions
//    val rech = new Rechunkifyer
//
//  val newBucketedData = rech.transform( options, bucketedData)
//      newBucketedData.show()

  }
}
