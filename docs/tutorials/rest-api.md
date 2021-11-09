---
layout: page
title: REST api interactions
categories: TUTORIAL HOWTOS
---

The following tutorial will help you to 


## Check REST api availability
Historian server should be started up on port 8080. 
You should so be able to browse to a status page about itself at [http://localhost:8080](http://localhost:8080/api/v1). 

```bash
curl -X GET http://localhost:8080/api/v1
> Historian PromQL api is working fine, enjoy!
```


## Injecting data from csv
Since you have been working ard on your [single node Historian setup](getting-started), you'll be rewarded by adding some data.
This can be done in several ways, but may be the simplest one is by importing a (small) csv file. 

> If you want to inject massive data files, yu should really consider using [spark API](spark-api)


```bash
curl --location --request POST 'localhost:8081/api/historian/v0/import/csv' \
--form 'my_csv_file=@samples/it-data-small.csv' \
--form 'mapping.name=metric' \
--form 'mapping.value=value' \
--form 'mapping.timestamp=timestamp' \
--form 'mapping.tags=host_id' \
--form 'group_by=name' \
--form 'group_by=tags.host_id' \
--form 'format_date=SECONDS_EPOCH' \
--form 'timezone_date=UTC'
```


`curl --location --request POST 'localhost:8081/api/v1/labels?start=1546372224&end=1636393824'`

```json
{
  "status": "success",
    "data": [
        "id",
        "host_id",
        "__name__"
    ]
}
```




`curl --location --request GET 'http://localhost:8081/api/v1/label/__name__/values?start=1478627424&end=1636393824'`

```json
{
    "status":"success",
    "data":[
        "memoryConsumed",
        "ack",
        "cpu",
        "cpu_ready",
        "messages",
        "consumers",
        "cpu_wait",
        "cpu_usage",
        "messages_ready",
        "messages_unacknowledged",
        "cpu_prct_used"
    ]
}
```



ack|id$089ab721585b4cb9ab20aaa6a13f08ea


## What's next
Now we have a basic understanding of REST api, you may ask where to go from there ?

- See how to handle big data workflows with [Spark API](spark-api)
- Dive in detail on REST api [Spark API](../api)
- Have a look to dataviz with [Grafana Prometheus](data-viz)