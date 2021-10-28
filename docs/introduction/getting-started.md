---
layout: page
title: Getting Started Guide
categories: TUTORIAL HOWTOS
---

The following tutorial will help you to try out the features of Historian in a few steps

> All is done here with single node setup, but you can also install all the components on a Kubernetes cluster through [Helm chart](kube-setup) or via [docker compose](docker-setup)

1. setup the single node environment (solr/grafana/spark/historian)
2. inject some data
3. search and retrieve data within the REST API
4. visualize data


### Prerequisites

- JDK 1.8+
- Apache Maven 3.6.0+
- Docker 20+

### Downloading Spark
[Apache Spark™](https://spark.apache.org/) is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

[Download Apache Spark](https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-without-hadoop.tgz) then extract it:
```shell
wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-without-hadoop.tgz
tar xvfz spark-*.tgz
rm spark-*.tgz
```

### Downloading SolR
[Apache SolR](https://solr.apache.org/) is the popular, blazing-fast, open source enterprise search platform built on Apache Lucene™. SolR is highly reliable, scalable and fault tolerant, providing distributed indexing, replication and load-balanced querying, automated failover and recovery, centralized configuration and more. Solr powers the search and navigation features of many of the world's largest internet sites.

[Download Apache SolR](https://archive.apache.org/dist/lucene/solr/8.9.0/solr-8.9.0.tgz), then extract it and start a solr node in cloud mode on port 8983:
```shell
wget https://archive.apache.org/dist/lucene/solr/8.9.0/solr-8.9.0.tgz
tar xvfz solr-*.tgz
rm solr-*.tgz
```

### Downloading Historian
[Download the latest release](https://github.com/Hurence/historian/releases/download/v1.3.8/historian-1.3.9-bin.tgz) of Historian then extract it:

```shell
wget https://github.com/Hurence/historian/releases/download/v1.3.8/historian-1.3.9-bin.tgz
tar xvfz historian-*.tgz
rm historian-*.tgz
```

### Downloading grafana
Centralize the analysis, visualization, and alerting for all of your data with Grafana.

[Download the latest release](https://grafana.com/grafana/download?edition=oss&platform=linux&plcmt=footer) of Grafana then follow the given instructions for your platform.

### Configuring Solr 
Before starting Historian we need to have a running solr cloud (even with one node), with a collection `historian` created from a working configset. Here are the commands to handle that :

```shell
# create a folder for solr data
mkdir -p data/solr/node1
cp historian-1.3.9/conf/solr.xml data/solr/node1
cp historian-1.3.9/conf/zoo.cfg data/solr/node1

# start a solr node and wait a little for the core to be initialized
solr-8.9.0/bin/solr start -cloud -s data/solr/node1/ -p 8983

# create config set archive
cd  historian-1.3.9/conf/solr/conf/ ; zip -r historian-configset.zip ./* ; cd -;

# upload configset to solr cloud
curl --location --request POST "http://localhost:8983/solr/admin/configs?action=UPLOAD&name=historian" \
            --header 'Content-Type: application/zip' \
            --data-binary '@historian-1.3.9/conf/solr/conf/historian-configset.zip'

# create the collection
curl --location --request POST "http://localhost:8983/v2/c" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "create": {
            "name": "historian",
            "config": "historian",
            "maxShardsPerNode": 6,
            "numShards": 3,
            "replicationFactor": 1
        }
    }'
```

Now if you go to [http://localhost:8983/solr/#/~cloud?view=graph](http://localhost:8983/solr/#/~cloud?view=graph) you should see an `historian` collection made of 3 shards on one Solr core. And if you want to have a look to the pre-defined schema let's open your browser with [http://localhost:8983/solr/#/historian/files?file=managed-schema](http://localhost:8983/solr/#/historian/files?file=managed-schema)


### Configuring Historian server
Historian server configuration is in [json](https://www.json.org/). The historian download comes with a sample configuration filecalled [conf/historian-server-conf.json](https://github.com/Hurence/historian/blob/master/historian-resources/conf/historian-server-conf.json) that is a good place to get started. For this setup you shouldn't have much to change but here is an excerpt of what could interest you in first place (in case you have changed some server ports for instance), other properties have been hidden to make it more succinct  :

```json
{
  "http_server" : {
    "host": "0.0.0.0",
    "port" : 8080
  },
  "historian": {
    "solr" : {
      "zookeeper_urls": ["localhost:9983"],
      "stream_url" : "http://localhost:8983/solr/historian",
      "chunk_collection": "historian"
    }
  }
}
```

### Starting Historian server
To start Historian server with our newly created configuration file run:

```bash
java -jar historian-1.3.9/lib/historian-server-1.3.9-fat.jar  -conf historian-1.3.9/conf/historian-server-conf.json 
```

Historian server should start up. You should also be able to browse to a status page about itself at http://localhost:8080. 

```bash
curl -X GET http://localhost:8080/api/v1
> Historian PromQL api is working fine, enjoy!
```


### Injecting data
Since you have been working ard on your single node Historian setup, you'll be rewarded by adding some data. this can be done in several ways, but may be the simplest one is by importing a csv file.


```bash
curl -X POST 'localhost:8080/api/historian/v0/import/csv' \
  --header 'Content-Type: application/json' \
  --form 'my_csv_file=@"/tmp/owid-covid-data.csv"' \
  --form 'mapping.name="iso_code"' \
  --form 'mapping.value="total_deaths_per_million"' \
  --form 'mapping.timestamp="date"' \
  --form 'mapping.tags="location"' \
  --form 'mapping.tags="continent"' \
  --form 'mapping.tags="iso_code"' \
  --form 'group_by="name"' \
  --form 'format_date="yyyy-MM-dd"' \
  --form 'timezone_date="UTC"' \
  --form 'max_number_of_lignes="200000"'
```