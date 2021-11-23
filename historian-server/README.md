# Historian server

This is the main piece of software to handle REST interactions with SolR. The main purpose is to query solr and to serialize / deserialize chunks for binary values stroed into solr documents.


## Build with maven

You need a JDK 8+ and maven, just run :
```shell
mvn clean install
```

require some other historian module, if it fails try this :
```shell
mvn -pl :historian-server -am clean install -DskipTests
```

## Run server on local

edit the file `historian-server.conf` starting with the one found in `historian-resources`. Pay attention to zookeeper urls for solr hosts. 
```shell script
java -jar target/historian-server-1.3.9-fat.jar  -conf ../historian-resources/conf/historian-server-conf.json 
```

## Run server with docker-compose

in logisland-quickstart project you can run a pipeline that injects data into solr.
You have access to solr UI, a grafana with a predefined historian dashboard.

in root folder of projct **logisland-quickstart** run :

```shell script
docker-compose -f docker-compose.yml -f docker-compose.historian.yml up -d
```

then follow the timeseries tutorial to inject timeseries : https://logisland.github.io/docs/guides/timeseries-guide

Go to grafana at http://localhost:3000. 
* user: admin
* mdp: admin.

Go to the historian dashboard and see your data ! We added three variables so that you can specify
the sampling algorithm to use, the bucket size or filter on a tag. Currently tags are just the name of the metric but we could
imagine tagging several different metric names with a same tag. For exemple 'temp' for metrics 'temp_a' and 'temp_b'.

By default no sampling is used if there is not too many measure to draw. Otherwise we calculate the bucket size depending on
the total number of measures that is being queried with the average algorithm. At the moment only basic algorithms are available.




to build integration tests source class ! Then you can run the test in your IDE.

## Configuration

You have an exemple in ./resources/config.json

Where every settings you can use are set. This files should always be up to date.

```json
{
  "web.verticles.instance.number": 2,
  "historian.verticles.instance.number": 1,
  "http_server" : {
    "host": "localhost",
    "port" : 8080,
    "historian.address": "historian",
    "max_data_points_allowed_for_ExportCsv" : 10000
  },
  "historian": {
    "address" : "historian",
    "schema_version": "VERSION_0",
    "limit_number_of_point_before_using_pre_agg" : 50000,
    "limit_number_of_chunks_before_using_solr_partition" : 50000,
    "sleep_milli_between_connection_attempt" : 10000,
    "number_of_connection_attempt" : 3,
    "api" : {
      "grafana" : {
        "search" : {
          "default_size": 2
        },
        "annotations" : {
          "limit": 100
        }
      }
    },
    "solr" : {
      "use_zookeeper": false,
      "zookeeper_urls": ["localhost:2181"],
      "stream_url": "http://localhost:8983/solr/historian",
      "chunk_collection": "historian",
      "annotation_collection": "annotation"
    }
  }
}
```

There is a part to configure the rest API called **"http_server"**

```json
{
    "host": "localhost",
    "port" : 8080,
    "historian.address": "historian",
    "max_data_points_allowed_for_ExportCsv" : 10000
}
```

There is a part to configure the historian service called **"historian"**

```json
 {
    "address" : "historian",
    "schema_version": "VERSION_0",
    "limit_number_of_point_before_using_pre_agg" : 50000,
    "limit_number_of_chunks_before_using_solr_partition" : 50000,
    "sleep_milli_between_connection_attempt" : 10000,
    "number_of_connection_attempt" : 3,
    "api" : {
      "grafana" : {
        "search" : {
          "default_size": 2
        },
        "annotations" : {
          "limit": 100
        }
      }
    },
    "solr" : {
      "use_zookeeper": false,
      "zookeeper_urls": ["localhost:2181"],
      "stream_url": "http://localhost:8983/solr/historian",
      "chunk_collection": "historian",
      "annotation_collection": "annotation"
    }
  }
```

If tou are working with the old schema please specify **"schema_version": "VERSION_0"** otherwise
by default it is using the last stable schema. At the moment there is only two different types of schemas.


