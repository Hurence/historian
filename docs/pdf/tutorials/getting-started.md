

# Getting Started with Hurence Data Historian

The following tutorial will help you to try out the features of Data Historian in a few steps

> All is done here with docker-compose but everything could be done and installed manually

1. setup the docker environement
2. inject some data
3. search and retrieve data within the REST API
4. visualize data
5. load data with spark


## Setup Docker Environement


### instal Solr

wget https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz


Introduction
ZooKeeper is an Apache Software Foundation project designed to simplify monitoring and managing group services. It uses a simple interface for its centralized coordination service that manages configuration, information, naming, distributed synchronization, and provisioning.

In this tutorial, learn how to install and set up Apache ZooKeeper on Ubuntu 18.04 or 20.04.

Install Apache ZooKeeper on Ubuntu.
Prerequisites
A Linux system running Ubuntu 20.04 or 18.04
Access to a terminal window/command line (Search > Terminal)
A user account with sudo or root privileges
Installing Apache ZooKeeper on Ubuntu
Step 1: Installing Java
ZooKeeper is written in Java and requires this programming language to work. To check whether Java is already installed, run the command:

    java --version

If the output displays a running Java version, you can move on to the next step. If the system says there is no such file or directory, you need to install Java before moving on to ZooKeeper.

There are different open-source Java packages available for Ubuntu. Find which one is best for you and its installation guide in How to Install Java on Ubuntu. The instructions apply to Ubuntu 18.04 and Ubuntu 20.04.




cat <<EOT >> /etc/default/solr.in.sh
ZK_HOST=10.20.20.142:2181,10.20.20.140:2181
SOLR_LOG_LEVEL=WARN
EOT



## Demo





### load data

first we load some it monitoring data to analyse an issue. we'll bucket just on day

* batch load som csv files "bin/historian-loader.sh start -c conf/fileloader-it-data.yml"
* have a look to yaml conf, envs files
* just check chunks into solr "metric_id:f5efa804-8b41-40c5-8101-34464c02fe7a"
* a facet on "chunk_count"
* sort by "chunk_day asc"
* analyze "chunk_sax,chunk_day,chunk_avg", is there a day which is different from the others ?
* filter on outliers and export them to csv with a tag
* do a simple dashboard on messages to show the peak on day 28
* launch clustering on this day
* show clustering result in dashboard grafana
* then load chemistry data "bin/historian-loader.sh start -c conf/fileloader-chemistry.yml"
* do a sinmple dashboard around the 2020-03-01



### play with spark API


* run a shell "../../spark-2.3.2-bin-hadoop2.7/bin/spark-shell --jars lib/historian-spark-1.3.6.jar --driver-memory 4g"
* imports all needed

```scala
import com.hurence.historian.service.SolrChunkService
import com.hurence.historian.spark.ml.{Chunkyfier, ChunkyfierStreaming,UnChunkyfier}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader.{ChunksReaderType, MeasuresReaderType, ReaderFactory}
import com.hurence.historian.spark.sql.writer.solr.SolrChunkForeachWriter
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
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

* just read som csv as Measures

```scala
/* a csv reader*/
val measuresDS = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV).read(sql.Options(
        "/Users/tom/Documents/data/chemistry/ISNTS35-N-2020-03/*.csv",
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


measuresDS.filter( r => r.getName() == "068_PI01").count
measuresDS.filter( r => r.getName() == "068_PI01").orderBy("timestamp").show
```

* now play with chunks

```scala
// transform Measures into Chunks
val chunkyfier = new Chunkyfier()
  .setOrigin("shell")
  .setGroupByCols("name".split(","))
  .setDateBucketFormat("yyyy-MM-dd.HH")
  .setSaxAlphabetSize(7)
  .setSaxStringLength(24)

val chunksDS = chunkyfier.transform(measuresDS).as[Chunk](Encoders.bean(classOf[Chunk])).cache()

chunksDS.filter( r => r.getName() == "068_PI01")
    .orderBy("start")
    .select( "day", "count", "avg", "sax")
    .show(false)
```




* now we'll load it data from solr

```scala
spark.catalog.clearCache


/* get data from solr*/
val solrDF = ReaderFactory.getChunksReader(ChunksReaderType.SOLR).read(sql.Options("historian", Map(
    "zkhost" -> "localhost:9983",
    "collection" -> "historian",
    "tag_names" -> "metric_id"
  ))).as[Chunk](Encoders.bean(classOf[Chunk])).cache()

val acksDS = solrDF.filter( r => r.getName() == "ack")
acksDS.filter( r => r.getTag("metric_id") == "dea8601d-8aa7-4c59-a3ce-99bbab8ac5ca").select("day", "avg", "count", "start", "sax").orderBy("day").show(false)


/* conversion to Measures */
val unchunkyfier = new UnChunkyfier()

val measuresDS = unchunkyfier.transform(acksDS).as[Measure](Encoders.bean(classOf[Measure])).cache()
measuresDS.filter( r => r.getTag("metric_id") == "dea8601d-8aa7-4c59-a3ce-99bbab8ac5ca").orderBy("timestamp").show(false)
```

* let's play with spark MLLib and Kmeans

```scala
/* Kmeans clustering*/

val tokenizer = new RegexTokenizer().setInputCol("sax").setOutputCol("words").setPattern("(?!^)")
val vectorizer = new CountVectorizer().setInputCol("words").setOutputCol("features")
val pipeline = new Pipeline().setStages(Array(tokenizer,vectorizer))

val splits = acksDS.randomSplit(Array(0.8, 0.2), 15L)
val (train, test) = (splits(0), splits(1))


val dataset = pipeline.fit(train).transform(train)
dataset.select("day","avg","tags","sax","features").show(false)

val kmeans = new KMeans().setK(5).setSeed(1L).setMaxIter(20)
val model = kmeans.fit(dataset)
val predictions = model.transform(dataset)
predictions.select("day", "avg","tags","sax","prediction").orderBy("prediction").show(300,false)
model.clusterCenters.foreach(println)
```



### the compactor

* launch the compactor to see that