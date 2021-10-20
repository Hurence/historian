# Getting Started

This tool is based on original source code from : [https://github.com/jmazzitelli/prometheus-scraper](https://github.com/jmazzitelli/prometheus-scraper)

This provides an API and a command line utility to scrape metrics from a Prometheus protocol endpoint. It supports both binary and text Prometheus data formats and send data into Historian


## Configuration

The following properties can be set in `application.properties` file or passed at command line or docker env 

```properties
# interval time in ms between scrap calls
scraper.scheduledDelayMs=${SCRAPER_DELAY:15000}
# the prometheus metrics url
scraper.url=${SCRAPPER_URL:http://localhost:9854/metrics}
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

## Docker 

Use the following commands to build `hurence/historian-scrapper` container.

```bash
mvn clean install
docker build --rm -t hurence/historian-scrapper .
docker tag hurence/historian-scrapper:latest hurence/historian-scrapper:x.x.x
docker push  hurence/historian-scrapper:x.x.x
```

run a scrapper on a prometheus url
```bash
docker run -p 8081:8081 --env SCRAPPER_URL=http://plouk:8080  hurence/historian-scrapper
```