---
layout: page
title: Spark API
categories: TUTORIAL HOWTOS
---

The following tutorial will show you how to read and write data through the Spark API of hurence Historian :

1. Cleanup previous data
2. Run a Spark shell
3. Load measures from CSV file
4. Create Chunks from Measures
5. Write Chunks to SolR Historian

## Introduction
Apache Spark is a parallel computation framework that helps to handle massive data mining workloads.
You write code in scala, python or java and the framework splits and executes the code in parallel over a cluster of nodes.
This is the preferred way to interact with Historian data at scale (way better than REST API which must be kept for mediu scale realtime interactions)

## Cleanup previous data
As we will load the same data (although much bigger) we can remove the small dataset previously loaded in [getting started guide](introduction/getting-started). This will be done directly in solr

```bash
curl -g "http://localhost:8983/solr/historian/update" \
    -H 'Content-Type: application/json' \
    -d '{"delete":{"query":"chunk_origin:ingestion-csv"}}'
```

## Run a spark shell
In the [getting started guide](introduction/getting-started) we have downloaded and set up spark and Hurence Historian.

You can then run a shell with Historian framework `$SPARK_HOME/bin/spark-shell --jars $HISTORIAN_HOME/lib/historian-spark-1.3.9.jar --driver-memory 4g`

imports all needed packages

```scala
import com.hurence.historian.service.SolrChunkService
import com.hurence.historian.spark.ml.{Chunkyfier, ChunkyfierStreaming,UnChunkyfier}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader._
import com.hurence.historian.spark.sql.writer.solr.SolrChunkForeachWriter
import com.hurence.historian.spark.sql.writer._
import com.hurence.timeseries.model.{Measure,Chunk}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.ml.feature.{VectorAssembler,NGram,RegexTokenizer, Tokenizer, CountVectorizer}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
```

## Load measures from CSV file

Here we will load the file `$HISTORIAN_HOME/samples/it-data.csv` which contains it monitoring data.
This is a comma separated file with timestamp in seconds, a metric name called `metric` and just one tag (or label) which is called `host_id`

```csv
timestamp,value,metric,host_id
1574654504,148.6,ack,089ab721585b4cb9ab20aaa6a13f08ea
1574688099,170.2,ack,089ab721585b4cb9ab20aaa6a13f08ea
1574706697,155.2,ack,089ab721585b4cb9ab20aaa6a13f08ea
1574739995,144.0,ack,089ab721585b4cb9ab20aaa6a13f08ea
```

We first start by just reading some csv lines and convert them as `Measures` :

```scala
/* a csv reader*/
val measuresDS = ReaderFactory.getMeasuresReader(ReaderType.CSV)
  .read(sql.Options(
        "samples/it-data.csv",
        Map(
          "inferSchema" -> "true",
          "delimiter" -> ",",
          "header" -> "true",
          "nameField" -> "metric",
          "timestampField" -> "timestamp",
          "timestampDateFormat" -> "s",
          "valueField" -> "value",
          "qualityField" -> "",
          "tagsFields" -> "host_id"
        ))).cache()

measuresDS.printSchema
measuresDS.filter( r => r.getName() == "cpu").count
measuresDS.filter( r => r.getName() == "cpu").orderBy("timestamp").show(false)
```

this creates a Spark Dataframe like the following :

```
+----+-------+---------------------------------------------+-------------+-----+
|name|quality|tags                                         |timestamp    |value|
+----+-------+---------------------------------------------+-------------+-----+
|cpu |NaN    |[host_id -> 4c1786a063d30d67cbcb6d69b0560078]|1574640038000|0.0  |
|cpu |NaN    |[host_id -> 535fc17be45e0f7e79a40549a41d7687]|1574640184000|10.0 |
|cpu |NaN    |[host_id -> e9bb5b34af52d9fdb748a9d5f2a0bfa5]|1574640197000|4.0  |
|cpu |NaN    |[host_id -> 4c1786a063d30d67cbcb6d69b0560078]|1574640338000|0.0  |
|cpu |NaN    |[host_id -> 535fc17be45e0f7e79a40549a41d7687]|1574640484000|10.0 |
|cpu |NaN    |[host_id -> e9bb5b34af52d9fdb748a9d5f2a0bfa5]|1574640497000|3.0  |
+----+-------+---------------------------------------------+-------------+-----+
```

## Create Chunks from Measures

Now to play with chunks we need a `Chunkyfier` object that will pack all `Measures` into `Chunks` grouping them (implicitly) by `name` and `tags.host_id` and bucketing them into a full day interval ("yyyy-MM-dd"), this could be done hourly with "yyyy-MM-dd.HH" pattern.
Moreover many aggregates are computed at chunk creation time (stats and sax encoding).

```scala
// setup the chunkyfier
val chunkyfier = new Chunkyfier()
  .setOrigin("shell")
  .setDateBucketFormat("yyyy-MM-dd")
  .setSaxAlphabetSize(5)
  .setSaxStringLength(24)

// transform Measures into Chunks
val chunksDS = chunkyfier.transform(measuresDS)
  .as[Chunk](Encoders.bean(classOf[Chunk]))
  .cache()

// print data
chunksDS.filter( r => r.getName() == "cpu")
    .orderBy("start")
    .select( "tags.host_id", "day", "count", "avg", "sax")
    .show(false)
```

We can have a look to the `Chunk` dataset created and those 24-character length sax ended strings (with a 5 letters alphabet)

```
+--------------------------------+----------+-----+------------------+------------------------+
|host_id                         |day       |count|avg               |sax                     |
+--------------------------------+----------+-----+------------------+------------------------+
|4c1786a063d30d67cbcb6d69b0560078|2019-11-25|287  |2.3972125435540073|bddbbbbbbbcbceedbbbbccbb|
|535fc17be45e0f7e79a40549a41d7687|2019-11-25|287  |9.989547038327526 |babbdddddccdcccdccbbcccb|
|e9bb5b34af52d9fdb748a9d5f2a0bfa5|2019-11-25|288  |3.6388888888888884|ccbcbcbcbcbbdccccdccbdce|
|4c1786a063d30d67cbcb6d69b0560078|2019-11-29|288  |2.21875           |eecbbccbbbbccbcbccbbbbbc|
|e9bb5b34af52d9fdb748a9d5f2a0bfa5|2019-11-29|288  |3.40625           |dcdcbbdcbcaccbcbcdcdcdcd|
|535fc17be45e0f7e79a40549a41d7687|2019-11-30|286  |9.881118881118882 |bbcdddddddccccccccbbbbbb|
|4c1786a063d30d67cbcb6d69b0560078|2019-11-30|287  |2.4390243902439033|ccbcbddcbbbbbbbbbceebbbb|
|e9bb5b34af52d9fdb748a9d5f2a0bfa5|2019-11-30|285  |3.424561403508772 |dedccdccbcccbaccccdbcbbc|
+--------------------------------+----------+-----+------------------+------------------------+

```

## Write Chunks to SolR Historian

Once we have those `Chunk` dataset created, it's really easy to store it into a SolR store

```scala
// write chunks to SolR
WriterFactory.getChunksWriter(WriterType.SOLR)
  .write(sql.Options("historian", Map(
    "zkhost" -> "localhost:9983",
    "collection" -> "historian"
  )), chunksDS)
```

## Loading Measures from solr Historian

Now we'll do the inverse operation: load data from solr.
Here we get `Chunk` data from solr and you'll want to *unchunkify* them to get atomic `Measure` points. 

please note that we filter our data with the `query` parameter (here we only get chunk from shell origin, aka those previously injected)
  
```scala
spark.catalog.clearCache

// get Chunks data from solr
val solrDF = ReaderFactory.getChunksReader(ReaderType.SOLR)
  .read(sql.Options("historian", Map(
    "zkhost" -> "localhost:9983",
    "collection" -> "historian",
    "query" -> "chunk_origin:shell"
  ))).as[Chunk](Encoders.bean(classOf[Chunk]))
  .cache()

// only keep `ack` metrics
val acksDS = solrDF.filter( r => r.getName() == "ack")

acksDS.select("day", "avg", "count", "start", "sax")
  .orderBy("day")
  .show(false)

// conversion back to Measures
val unchunkyfier = new UnChunkyfier()

val measuresDS = unchunkyfier.transform(acksDS).as[Measure](Encoders.bean(classOf[Measure])).cache()
measuresDS.filter( r => r.getTag("host_id") == "aa27ac7bc75f3afc34849a60a9c5f62c").orderBy("timestamp").show(false)
```
and we get back our measures ...

```
+--------------------------------------------+----+-------+-------------+-----+
|metricKey                                   |name|quality|timestamp    |value|
+--------------------------------------------+----+-------+-------------+-----+
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574779331000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574779631000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574779931000|0.2  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574780231000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574780531000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574780831000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574781131000|0.2  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574781431000|0.4  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574781731000|0.2  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574782031000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574782331000|0.4  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574782631000|1.6  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574782931000|0.2  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574783231000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574783531000|0.0  |
|ack|host_id$aa27ac7bc75f3afc34849a60a9c5f62c|ack |NaN    |1574783831000|0.0  |
```


## What's next
Now we have a basic understanding of Spark API, you may ask where to go from there ?

- Go deeper with Spark MLLib by [clustering timeseries](clustering)
- See how to do realtime interactions through [REST API](rest-api)
- Have a look to dataviz with [Grafana Prometheus](data-viz)
