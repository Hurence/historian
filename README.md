# Hurence Data Historian (HDH)

timeseries big data analytics tools


## A few concepts
Hurence Data Historian is a free solution to handle massive loads of timeseries data into a search engine (such as Apache SolR).
The key concepts are simple :

- **Measure** is a point in time with a floating point value identified by a name and some tags (categorical features)
- **Chunk** is a set of contiguous Measures with a time interval grouped by a date bucket, the measure name and eventually some tags

The main purpose of this tool is to help creating, storing and retrieving these chunks of timeseries.
We use chunking instead of raw storage in order to save some disk space and reduce costs at scale. Also chunking is very usefull to pre-compute some arggregation and to facilitate down-sampling

### Data model

A Measure point is identified by the following fields
```scala
case class Measure(name: String,
                   value: Double,
                   timestamp: Long,
                   year: Int,
                   month: Int,
                   day: String,
                   hour: Int,
                   tags: Map[String,String])
```


A Chunk is identified by the following fields
```scala
case class Chunk(name: String,
     day:String,
     start: Long,
     end: Long,
     chunk: String,
     count: Long,
     avg: Double,
     stddev: Double,
     min: Double,
     max: Double,
     first: Double,
     last: Double,
     sax: String,
     tags: Map[String,String])
```
As you can see from a Measure points to a Chunk of Measures, the `timestamp` field has been replaced by a `start` and `stop` interval and the `value` is now a base64 encoded string named `chunk`.
 

In SolR the chunks will be stored according to  the following schema

```xml
    <field name="chunk_avg" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_value" type="string" multiValued="false" indexed="false"/>
    <field name="chunk_count" type="pint" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_day" type="string" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_end" type="plong" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_first" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_hour" type="pint" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_last" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_max" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_min" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_month" type="pint" multiValued="false" indexed="true" stored="true"/>
    <field name="name" type="string" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_outlier" type="boolean" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_qualities" type="string" multiValued="true" indexed="true" stored="true"/>
    <field name="chunk_sax" type="ngramtext" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_start" type="plong" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_stddev" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_sum" type="pdouble" multiValued="false" indexed="true" stored="true"/>
    <field name="tags" type="text_en" multiValued="true" indexed="true" stored="true"/>
    <field name="chunk_timestamp" type="plong" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_trend" type="boolean" multiValued="false" indexed="true" stored="true"/>
    <field name="chunk_year" type="pint" multiValued="false" indexed="true" stored="true"/>
```


## Setup


### Workspace boostrap
make a workspace directory called `hdh_workspace` for example. we'll refer to it as `$HDH_HOME`
The following commands will bootstrap your setup with folder and conf files needed for SolR setup:


```bash
# create the workspace anywhere you want
mkdir ~/hdh_workspace
export HDH_HOME=~/hdh_workspace

# create 2 data folders for SolR data
mkdir -p $HDH_HOME/data/solr/node1 $HDH_HOME/data/solr/node2

# touch a few config files for SolR
SOLR_XML='<?xml version="1.0" encoding="UTF-8" ?>
      <solr>
        <int name="maxBooleanClauses">${solr.max.booleanClauses:1024}</int>
        <solrcloud>
          <str name="host">${host:}</str>
          <int name="hostPort">${jetty.port:8983}</int>
          <str name="hostContext">${hostContext:solr}</str>
      
          <bool name="genericCoreNodeNames">${genericCoreNodeNames:true}</bool>
      
          <int name="zkClientTimeout">${zkClientTimeout:30000}</int>
          <int name="distribUpdateSoTimeout">${distribUpdateSoTimeout:600000}</int>
          <int name="distribUpdateConnTimeout">${distribUpdateConnTimeout:60000}</int>
          <str name="zkCredentialsProvider">${zkCredentialsProvider:org.apache.solr.common.cloud.DefaultZkCredentialsProvider}</str>
          <str name="zkACLProvider">${zkACLProvider:org.apache.solr.common.cloud.DefaultZkACLProvider}</str>
        </solrcloud>
        <shardHandlerFactory name="shardHandlerFactory"
          class="HttpShardHandlerFactory">
          <int name="socketTimeout">${socketTimeout:600000}</int>
          <int name="connTimeout">${connTimeout:60000}</int>
          <str name="shardsWhitelist">${solr.shardsWhitelist:}</str>
        </shardHandlerFactory>
      </solr>'  
      
echo ${SOLR_XML} > ${HDH_HOME}/data/solr/node1/solr.xml 
echo ${SOLR_XML} > ${HDH_HOME}/data/solr/node2/solr.xml

touch ${HDH_HOME}/data/solr/node1/zoo.cfg
```


### Install Apache SolR
Apache SolR is the underlying datastore of HDH, it can be replaced by any other search engine or even S3 storage.

To follow along with this tutorial, you will need to unpack the following solr archive : [https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz](https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz) into your `$HDH_HOME` directory and then start 2 solr cores locally with data store into `$HDH_HOME/data` folder

```bash
# get SolR 8.2.0 and unpack it
cd $HDH_HOME
wget https://archive.apache.org/dist/lucene/solr/8.2.0/solr-8.2.0.tgz
tar -xvf solr-8.2.0.tgz
rm solr-8.2.0.tgz

# start a SolR cluster locally with an embedded zookeeper
cd $HDH_HOME/solr-8.2.0
bin/solr start -cloud -s $HDH_HOME/data/solr/node1  -p 8983
bin/solr start -cloud -s $HDH_HOME/data/solr/node2/  -p 7574 -z localhost:9983
```

Please verify that you have two running SolR core by browsing solr admin at [http://localhost:8983/solr/#/~cloud](http://localhost:8983/solr/#/~cloud)

### Install Hurence Data Historian
Hurence Data Historian is a set of scripts and binaries that get you work with timeseries Chunks

you will need to unpack the following HDH archive : [https://github.com/Hurence/historian/releases/download/v1.3.4/historian-1.3.4-SNAPSHOT.tgz](https://github.com/Hurence/historian/releases/download/v1.3.4/historian-1.3.4-SNAPSHOT.tgz)  into your `$HDH_HOME` directory

you can now setup Hurence Data Historian with your working SolR instances

```bash
# get Hurence Historian and unpack it
cd $HDH_HOME
wget https://github.com/Hurence/historian/releases/download/v1.3.4/historian-1.3.4-SNAPSHOT.tgz
tar -xvf historian-1.3.4-SNAPSHOT.tgz
rm historian-1.3.4-SNAPSHOT.tgz

# create the HDH schema and collection in SolR
cd $HDH_HOME/historian-1.3.4-SNAPSHOT
bin/create-historian-chunk-collection.sh
    
# and launch the historian REST server
bin/historian-server.sh start
```




### Install Grafana
Install Grafana for your platform as described here : `https://grafana.com/docs/grafana/latest/installation/requirements/ `
Install Json Simple Datasource as described here : 

https://grafana.com/grafana/plugins/grafana-simple-json-datasource


To install this plugin using the grafana-cli tool:

    sudo grafana-cli plugins install grafana-simple-json-datasource
    sudo service grafana-server restart

    
### Install Apache Spark
This step is not mandatory if you don't want to process your data with spark framework for big data analysis.
However to setup spark, you'll need to unpack the following Spark archive : [https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz](https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz)

the following commands will guide you to install a local setup into 

```bash
# get Apache Spark 2.3.4 and unpack it
cd $HDH_HOME
wget https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-without-hadoop.tgz
tar -xvf spark-2.3.4-bin-without-hadoop.tgz
rm spark-2.3.4-bin-without-hadoop.tgz


# add two additional jars to spark to handle our framework
wget -O spark-solr-3.6.6-shaded.jar https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/3.6.6/spark-solr-3.6.6-shaded.jar
mv spark-solr-3.6.6-shaded.jar $HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/ 
cp $HDH_HOME/historian-1.3.4-SNAPSHOT/lib/loader-1.3.4-SNAPSHOT.jar $HDH_HOME/spark-2.3.4-bin-without-hadoop/jars/ 

```




## Tear down and cleanup
This section shows how to stop the services and cleanup data if needed.
    
### Stop your SolR instances

when you're done you can stop your SolR cores
    
    cd $HDH_HOME/solr-8.2.0
    bin/solr stop -all
    
if you want to reset manually your data
    
    rm -r $HDH_HOME/data/solr/node1 $HDH_HOME/data/solr/node2
    
don't forget then to bootstrap your setup again as described previously.





# Data management
Now that we have a working setup of HDH we can start to play with data. There are several entry points depneding on your culture and needs.

- the REST API served by 


## Load data from REST API






```json
curl --location --request POST 'http://localhost:8080/api/grafana/query' \
--header 'Content-Type: application/json' \
--data-raw '{
  "panelId": 1,
  "range": {
    "from": "2019-03-01T00:00:00.000Z",
    "to": "2020-03-01T23:59:59.000Z"
  },
  "interval": "30s",
  "intervalMs": 30000,
  "targets": [
    {
      "target": "\"ack\"",
      "type": "timeserie"
    }
  ],
  "format": "json",
  "maxDataPoints": 550
}'
```


Get all metrics names

```bash
curl --location --request POST 'http://localhost:8080/api/grafana/search' \
--header 'Content-Type: application/json' \
--data-raw '{
    "target": "*"
}'

# ["ack", ... ,"messages","cpu"]

```

Get some measures points within a time range

```bash
curl --location --request POST 'http://localhost:8080/api/grafana/query' \
--header 'Content-Type: application/json' \
--data-raw '{
	 "range": {
          "from": "2019-11-25T23:59:59.999Z",
          "to": "2019-11-30T23:59:59.999Z"
      },
      "targets": [
        { "target": "ack" }
      ]
}'
```

## Retrieve data from REST API

## Use Spark to get data
Apache Spark is an Open Source framework designed to process hudge datasets in parallel on computing clusters.
Hurence Data Historian is highly integrated with Spark so that you can handle dataset interactions in both ways (input and output) through a simple API.

The following commands show you how to take a CSV dataset from HDFS or local filesystem, load it as a HDH

../_setup_historian/spark-2.3.4-bin-hadoop2.7/bin/spark-shell --jars assembly/target/historian-1.3.4-SNAPSHOT/historian-1.3.4-SNAPSHOT//lib/loader-1.3.4-SNAPSHOT.jar,assembly/target/historian-1.3.4-SNAPSHOT/historian-1.3.4-SNAPSHOT/lib/spark-solr-3.6.6-shaded.jar



```scala
import com.hurence.historian.model.ChunkRecordV0
import com.hurence.historian.spark.ml.Chunkyfier
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.functions._
import com.hurence.historian.spark.sql.reader.{MeasuresReaderType, ReaderFactory}
import com.hurence.historian.spark.sql.writer.{WriterFactory, WriterType}
import com.lucidworks.spark.util.SolrSupport
import org.apache.commons.cli.{DefaultParser, Option, Options}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

 val filePath = "/Users/tom/Documents/workspace/historian/loader/src/test/resources/it-data-4metrics.csv.gz"
 val reader = ReaderFactory.getMeasuresReader(MeasuresReaderType.GENERIC_CSV)
    val measuresDS = reader.read(sql.Options(
      filePath,
      Map(
        "inferSchema" -> "true",
        "delimiter" -> ",",
        "header" -> "true",
        "nameField" -> "metric_name",
        "timestampField" -> "timestamp",
        "timestampDateFormat" -> "s",
        "valueField" -> "value",
        "tagsFields" -> "metric_id,warn,crit"
      )))
      
      
      
val chunkyfier = new Chunkyfier().setGroupByCols(Array(  "name", "tags.metric_id"))
val chunksDS = chunkyfier.transform(measuresDS).as[ChunkRecordV0]
      
      
val writer = WriterFactory.getChunksWriter(WriterType.SOLR)
    writer.write(sql.Options("historian", Map(
      "zkhost" -> "localhost:9983",
      "collection" -> "historian",
      "tag_names" -> "metric_id,warn,crit"
    )), chunksDS)
```
## Visualize data through grafana

## Realtime data ingestion with logisland










# Development

Please see our documentation [here](DEVELOPMENT.md)

## Build project
This section is mainly for developers as it will guive some insight about compiling testing the framework.
Hurence Historian is Open Source, distributed as Apache 2.0 licence and the source repository is hosted on github at [https://github.com/Hurence/historian](https://github.com/Hurence/historian)

Run the following command in the root directory of historian source checkout.

    git clone git@github.com:Hurence/historian.git
    cd historian
    mvn clean install -DskipTests -Pbuild-integration-tests

## Install datasource plugin in your grafana instance

You just need to copy the plugin folder **./grafana-historian-dataosurce** folder of the plugin into the plugin folder of your grafana instances.
Look at your grafana.ini file, by default the path is **./data/plugins/**.

So you could do something like
 
 ``` shell script
cp -r ./grafana-historian-dataosurce ${GRAFANA_HOME}/data/plugins/
```

You need to restart your grafana server so that the changes are taking in account.










