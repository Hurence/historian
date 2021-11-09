---
layout: page
title: REST api interactions
categories: TUTORIAL HOWTOS
---

The following tutorial will help you to interact with historian through REST calls.


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

> If you want to inject massive data files, you should really consider using [spark API](spark-api)


Here we will load the file `$HISTORIAN_HOME/samples/it-data-small.csv` which contains it monitoring data.
This is a comma separated file with timestamp in seconds, a metric name called `metric` and just one tag (or label) which is called `host_id`

```csv
timestamp,value,metric,host_id
1574654504,148.6,ack,089ab721585b4cb9ab20aaa6a13f08ea
1574688099,170.2,ack,089ab721585b4cb9ab20aaa6a13f08ea
1574706697,155.2,ack,089ab721585b4cb9ab20aaa6a13f08ea
1574739995,144.0,ack,089ab721585b4cb9ab20aaa6a13f08ea
```

here is the REST endpoint

```bash
curl --location --request POST 'localhost:8080/api/historian/v0/import/csv' \
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


## Get all metric names
For a given time range you can retrieve all different metric names :

`curl --location --request GET 'http://localhost:8080/api/v1/label/__name__/values?start=1478627424&end=1636393824'`

here is the list 

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

## Get tags names
Tags or labels can be retrieved with the following query

`curl --location --request POST 'localhost:8080/api/v1/labels?start=1546372224&end=1636393824'`

here we can see that for this tie range and for all metrics we have only `host_id` tag available

```json
{
  "status": "success",
    "data": [
        "host_id",
        "__name__"
    ]
}
```


## Get possible tags values
Tags value (for a given tag) can be retrieved with the following query, for example to get all different `host_id`

`curl --location -g --request GET 'http://localhost:8080/api/v1/label/host_id/values?start=1546372224&end=1636393824'`

here is an portion of the return 

```json
{
    "status": "success",
    "data": [
        "7fa612ce947c19936edfbec3df915f71",
        "e3633124b3a1cc8c9a745cf3f287a5de",
        "b3d8062cc1f87acdd87dd5ab48343ca8",
        "1da5880b4325221336f1845ad48af50f",
        "e3b49519c186bc8000601e5736701090",
        "1000426d4e4674915f4be354ff712fbc",
        "10ef1c484d7f116564b4af2a9cbd0010",
        "343e2a8014354a3736a48f345f65fb04",
        "c654a9de55e9891a51c3ec351c410e32",
        "962473de4f5a94f4fa5954f343dc54de",
        "8686f5dd7307bf228726d29b4a52cf8b",
        "68a1d95a9428a653ee48899f9a3ae08b"
    ]
}
```



## Get metric value
And finally you all want to get some data out of the historian, for exemple `ack` metric in a small time range :

```bash 
curl --location --request POST 'localhost:8080/api/v1/query_range' \
    --header 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'query=ack' \
    --data-urlencode 'start=1574640104' \
    --data-urlencode 'end=1574642504'
```



And we get all the values you want

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "ack",
          "host_id": "089ab721585b4cb9ab20aaa6a13f08ea"
        },
        "values": [
          [
            1574640104,
            132.4
          ],
        
          [
            1574642204,
            147.4
          ],
          [
            1574642504,
            143.8
          ]
        ]
      },
      {
        "metric": {
          "__name__": "ack",
          "host_id": "089df6709ee37bb3bf1312277a2bf730"
        },
        "values": [
          [
            1574640184,
            0.0
          ],
         
          [
            1574642284,
            0.0
          ]
        ]
      }
    ]
  }
}
```

## What's next
Now we have a basic understanding of REST api, you may ask where to go from there ?

- See how to handle big data workflows with [Spark API](spark-api)
- Dive into the details of REST api [Spark API](../api)
- Have a look to dataviz with [Grafana Prometheus](data-viz)