# Historian scraper

This provides an API and a command line utility to scrape metrics from a Prometheus protocol endpoint. It supports both binary and text Prometheus data formats and send data into Historian

## Credits
This tool is based on original source code from : [https://github.com/jmazzitelli/prometheus-scraper](https://github.com/jmazzitelli/prometheus-scraper)


## setup

First create a Solr collection called `greensights` to store all custom data.

```bash
curl --location --request POST 'http://localhost:8983/v2/c' \
--header 'Content-Type: application/json' \
--data-raw '{
    "create": {
        "name": "greensights",
        "config": "_default",
        "numShards": 1,
        "replicationFactor": 2,
        "maxShardsPerNode": 3
    }
}'
```

make sure it has been created correctly 

```bash
curl --location --request GET 'http://localhost:8983/solr/admin/collections?action=COLSTATUS&collection=greensights&sizeInfo=true'
```


## Configuration

The following properties can be set in `application.properties` file or passed at command line or docker env 

```properties
# interval time in ms between scrap calls
scraper.scheduledDelayMs=${SCRAPER_DELAY:15000}
# the prometheus metrics url
scraper.url=${SCRAPER_URL:http://localhost:9854/metrics}
# historian zookeeper hosts quorum (without root)
historian.solr.zkHosts=${ZK_HOSTS:localhost:9983}
# historian zookeeper root
historian.solr.zkChroot=${ZK_CHROOT:}
# historian solr collection
historian.solr.collection=${COLLECTION:historian}
# solr batch udaters internal queue size
historian.solr.queueSize=${QUEUE_SIZE:10000}
# solr batch injection size
historian.solr.batchSize=${BATCH_SIZE:200}
# solr batch fluch interval in ms
historian.solr.flushIntervalMs=${FLUSH_INTERVAL:2000}
# historian chunk_origin field
historian.solr.chunkOrigin=${CHUNK_ORIGIN:prometheus-scrapper}
```