#Build historian gateway

run :
```shell script
mvn clean install
```

require some other logisland module, if it fails try this :

```shell script
mvn -pl :logisland-gateway-historian -am clean install -DskipTests
```

#Run server on local

run :
```shell script
n
```

#Run server with docker-compose

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

By default no sampling is used if there is not too many point to draw. Otherwise we calculate the bucket size depending on
the total number of points that is being queried with the average algorithm. At the moment only basic algorithms are available.

#Run server on cluster

TODO
```shell script
java -jar <jar_path> -cluster -conf <conf_path>
```

#RUN TEST

## In terminal

```shell script
mvn clean install -Pintegration-tests
``` 

## with your IDE

mark the folder ./src/integration-test/java as source test code in your IDE.
mark the folder ./src/integration-test/resources as resources test in your IDE.

Then run :
```shell script
mvn clean install -Pbuild-integration-tests
``` 

to build integration tests source class ! Then you can run the test in your IDE.

# Configuration

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


# CONTRIBUTE

Please read DEVELOPMENT.md

#TROUBLESHOOT

When code generator fails. It may be because of hidden char ? This is really irritating.
I fought with javadoc... Sometimes I could not succeed into making it working.