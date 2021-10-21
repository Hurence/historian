---
layout: page
title: clustering timeseries
---



Dataset de Chunk/Measures. Lancer un shell Spark avec la librairie historian-spark

    /usr/hdp/current/spark2-client/bin/spark-shell  \
        --jars lib/historian-spark-1.3.6-SNAPSHOT.jar --driver-memory 4g

Importer toutes les classes de l'api Historian.

```scala
/**
 * First let's import all needed packages
 */
import com.hurence.historian.service.SolrChunkService
import com.hurence.historian.spark.ml._
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader._
import com.hurence.historian.spark.sql.writer.solr.SolrChunkForeachWriter
import com.hurence.historian.spark.sql.writer._
import com.hurence.timeseries.model._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.ml.feature.{Tokenizer, CountVectorizer}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
```

Lire des Measures depuis des csv

```scala
// Create a csv reader and load all data from this CSV
val measuresDS = ReaderFactory.getMeasuresReader(ReaderType.CSV)
    .read(sql.Options(
        "hdfs://ifpen/user/hurence/evoa/historian-archive/ISNTS35-N-2021-01/*.csv.gz",
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ";",
          "header" -> "true",
          "nameField" -> "tagname",
          "timestampField" -> "timestamp",
          "timestampDateFormat" -> "dd/MM/yyyy HH:mm:ss",
          "valueField" -> "value",
          "qualityField" -> "quality",
          "tagsFields" -> "tagname"
        ))).cache()


// get only measures from a sensor
measuresDS.filter( r => r.getName() == "068_PI01").count
measuresDS.filter( r => r.getName() == "068_PI01").orderBy("timestamp").show
```

convertir ces Measures en Chunks

```scala
// transform Measures into 1h-length Chunks for each name
val chunkyfier = new Chunkyfier()
  .setOrigin("shell")
  .setGroupByCols("name".split(","))
  .setDateBucketFormat("yyyy-MM-dd.HH")
  .setSaxAlphabetSize(7)
  .setSaxStringLength(24)

val chunksDS = chunkyfier.transform(measuresDS)
    .as[Chunk](Encoders.bean(classOf[Chunk]))
    .cache()

chunksDS.filter( r => r.getName() == "068_PI01")
    .orderBy("start")
    .select( "day", "count", "avg", "sax")
    .show(false)
```

charger ces données depuis SolR manuellement

```scala
spark.catalog.clearCache


// get data from solr
val chunksDS = ReaderFactory.getChunksReader(ReaderType.SOLR)
    .read(sql.Options("historian-v3", Map(
        "zkhost" -> "islin-hdplnod10.ifp.fr:2181,islin-hdplnod11.ifp.fr:2181," +
            "islin-hdplnod12.ifp.fr:2181/solr",
        "collection" -> "historian-v3",
        "tag_names" -> "tagname"
    )))
    .as[Chunk](Encoders.bean(classOf[Chunk]))
    .filter( r => r.getName() == "068_PI01")
    .cache()

chunksDS.show(false)


// conversion to Measures
val unchunkyfier = new UnChunkyfier()

val measuresDS = unchunkyfier.transform(chunksDS)
    .as[Measure](Encoders.bean(classOf[Measure])).cache()
measuresDS.orderBy("timestamp").show(false)
```

Analyser les données avec MLLib. let's play with spark MLLib and Kmeans

```scala
/* Kmeans clustering*/

val tokenizer = new RegexTokenizer()
    .setInputCol("sax")
    .setOutputCol("words")
    .setPattern("(?!^)")
val vectorizer = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("features")
val pipeline = new Pipeline()
    .setStages(Array(tokenizer,vectorizer))

val splits = measuresDS.randomSplit(Array(0.8, 0.2), 15L)
val (train, test) = (splits(0), splits(1))


val dataset = pipeline.fit(train).transform(train)
dataset.select("day","avg","tags","sax","features").show(false)

val kmeans = new KMeans().setK(5).setSeed(1L).setMaxIter(20)
val model = kmeans.fit(dataset)
val predictions = model.transform(dataset)
predictions.select("day", "avg","tags","sax","prediction")
    .orderBy("prediction")
    .show(300,false)
model.clusterCenters.foreach(println)
```