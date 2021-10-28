---
title: HTTP API
sort_rank: 7
layout: page
---


The current stable HTTP API is reachable under `/api/v1` on a Historian server. 
Any non-breaking additions will be added under that endpoint.

## Format overview

The API response format is JSON. Every successful API request returns a `2xx`
status code.

Invalid requests that reach the API handlers return a JSON error object
and one of the following HTTP response codes:

- `400 Bad Request` when parameters are missing or incorrect.
- `422 Unprocessable Entity` when an expression can't be executed
  ([RFC4918](https://tools.ietf.org/html/rfc4918#page-78)).
- `503 Service Unavailable` when queries time out or abort.

Other non-`2xx` codes may be returned for errors occurring before the API
endpoint is reached.

An array of warnings may be returned if there are errors that do
not inhibit the request execution. All of the data that was successfully
collected will be returned in the data field.

The JSON response envelope format is as follows:

```json
{
  "status": "success" | "error",
  "data": <data>,

  // Only set if status is "error". The data field may still hold
  // additional data.
  "errorType": "<string>",
  "error": "<string>",

  // Only if there were warnings while executing the request.
  // There will still be data in the data field.
  "warnings": ["<string>"]
}
```

Generic placeholders are defined as follows:

* `<rfc3339 | unix_timestamp>`: Input timestamps may be provided either in
  [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) format or as a Unix timestamp
  in seconds, with optional decimal places for sub-second precision. Output
  timestamps are always represented as Unix timestamps in seconds.
* `<series_selector>`: Prometheus [time series
  selectors](basics.md#time-series-selectors) like `http_requests_total` or
  `http_requests_total{method=~"(GET|POST)"}` and need to be URL-encoded.
* `<duration>`: [Prometheus duration strings](basics.md#time_durations).
  For example, `5m` refers to a duration of 5 minutes.
* `<bool>`: boolean values (strings `true` and `false`).

Note: Names of query parameters that may be repeated end with `[]`.

## Expression queries

Query language expressions may be evaluated at a single instant or over a range
of time. The sections below describe the API endpoints for each type of
expression query.

### Instant queries

The following endpoint evaluates an instant query at a single point in time:

```
GET /api/v1/query
POST /api/v1/query
```

URL query parameters:

- `query=<string>`: Prometheus expression query string.
- `time=<rfc3339 | unix_timestamp>`: Evaluation timestamp. Optional.
- `timeout=<duration>`: Evaluation timeout. Optional. Defaults to and
  is capped by the value of the `-query.timeout` flag.

The current server time is used if the `time` parameter is omitted.

You can URL-encode these parameters directly in the request body by using the `POST` method and
`Content-Type: application/x-www-form-urlencoded` header. This is useful when specifying a large
query that may breach server-side URL character limits.

The `data` section of the query result has the following format:

```
{
  "resultType": "matrix" | "vector" | "scalar" | "string",
  "result": <value>
}
```

`<value>` refers to the query result data, which has varying formats
depending on the `resultType`. See the [expression query result
formats](#expression-query-result-formats).

The following example evaluates the expression `up` at the time
`2015-07-01T20:10:51.781Z`:

```bash
$ curl 'localhost:8080/api/v1/query?query=up&time=2015-07-01T20:10:51.781Z'
{
   "status" : "success",
   "data" : {
      "resultType" : "vector",
      "result" : [
         {
            "metric" : {
               "__name__" : "up",
               "job" : "prometheus",
               "instance" : "localhost:9090"
            },
            "value": [ 1435781451.781, "1" ]
         },
         {
            "metric" : {
               "__name__" : "up",
               "job" : "node",
               "instance" : "localhost:9100"
            },
            "value" : [ 1435781451.781, "0" ]
         }
      ]
   }
}
```

### Range queries

The following endpoint evaluates an expression query over a range of time:

```
GET /api/v1/query_range
POST /api/v1/query_range
```

URL query parameters:

- `query=<string>`: Prometheus expression query string.
- `start=<rfc3339 | unix_timestamp>`: Start timestamp, inclusive.
- `end=<rfc3339 | unix_timestamp>`: End timestamp, inclusive.
- `step=<duration | float>`: Query resolution step width in `duration` format or float number of seconds.
- `timeout=<duration>`: Evaluation timeout. Optional. Defaults to and
  is capped by the value of the `-query.timeout` flag.

You can URL-encode these parameters directly in the request body by using the `POST` method and
`Content-Type: application/x-www-form-urlencoded` header. This is useful when specifying a large
query that may breach server-side URL character limits.

The `data` section of the query result has the following format:

```
{
  "resultType": "matrix",
  "result": <value>
}
```

For the format of the `<value>` placeholder, see the [range-vector result
format](#range-vectors).

The following example evaluates the expression `up` over a 30-second range with
a query resolution of 15 seconds.

```bash
$ curl 'localhost:8080/api/v1/query_range?query=up&start=2015-07-01T20:10:30.781Z&end=2015-07-01T20:11:00.781Z&step=15s'
{
   "status" : "success",
   "data" : {
      "resultType" : "matrix",
      "result" : [
         {
            "metric" : {
               "__name__" : "up",
               "job" : "prometheus",
               "instance" : "localhost:9090"
            },
            "values" : [
               [ 1435781430.781, "1" ],
               [ 1435781445.781, "1" ],
               [ 1435781460.781, "1" ]
            ]
         },
         {
            "metric" : {
               "__name__" : "up",
               "job" : "node",
               "instance" : "localhost:9091"
            },
            "values" : [
               [ 1435781430.781, "0" ],
               [ 1435781445.781, "0" ],
               [ 1435781460.781, "1" ]
            ]
         }
      ]
   }
}
```

## Querying metadata

Prometheus offers a set of API endpoints to query metadata about series and their labels.

NOTE: These API endpoints may return metadata for series for which there is no sample within the selected time range, and/or for series whose samples have been marked as deleted via the deletion API endpoint. The exact extent of additionally returned series metadata is an implementation detail that may change in the future.

### Finding series by label matchers

The following endpoint returns the list of time series that match a certain label set.

```
GET /api/v1/series
POST /api/v1/series
```

URL query parameters:

- `match[]=<series_selector>`: Repeated series selector argument that selects the
  series to return. At least one `match[]` argument must be provided.
- `start=<rfc3339 | unix_timestamp>`: Start timestamp.
- `end=<rfc3339 | unix_timestamp>`: End timestamp.

You can URL-encode these parameters directly in the request body by using the `POST` method and
`Content-Type: application/x-www-form-urlencoded` header. This is useful when specifying a large
or dynamic number of series selectors that may breach server-side URL character limits.

The `data` section of the query result consists of a list of objects that
contain the label name/value pairs which identify each series.

The following example returns all series that match either of the selectors
`up` or `process_start_time_seconds{job="prometheus"}`:

```bash
$ curl 'localhost:8080/api/v1/series?' --data-urlencode 'match[]=up' --data-urlencode 'match[]=process_start_time_seconds{job="prometheus"}'
{
   "status" : "success",
   "data" : [
      {
         "__name__" : "up",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      },
      {
         "__name__" : "up",
         "job" : "node",
         "instance" : "localhost:9091"
      },
      {
         "__name__" : "process_start_time_seconds",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      }
   ]
}
```

### Getting label names

The following endpoint returns a list of label names:

```
GET /api/v1/labels
POST /api/v1/labels
```

URL query parameters:

- `start=<rfc3339 | unix_timestamp>`: Start timestamp. Optional.
- `end=<rfc3339 | unix_timestamp>`: End timestamp. Optional.
- `match[]=<series_selector>`: Repeated series selector argument that selects the
  series from which to read the label names. Optional.


The `data` section of the JSON response is a list of string label names.

Here is an example.

```bash
$ curl 'localhost:8080/api/v1/labels'
{
    "status": "success",
    "data": [
        "__name__",
        "call",
        "code",
        "config",
        "dialer_name",
        "endpoint",
        "event",
        "goversion",
        "handler",
        "instance",
        "interval",
        "job",
        "le",
        "listener_name",
        "name",
        "quantile",
        "reason",
        "role",
        "scrape_job",
        "slice",
        "version"
    ]
}
```

### Querying label values

The following endpoint returns a list of label values for a provided label name:

```
GET /api/v1/label/<label_name>/values
```

URL query parameters:

- `start=<rfc3339 | unix_timestamp>`: Start timestamp. Optional.
- `end=<rfc3339 | unix_timestamp>`: End timestamp. Optional.
- `match[]=<series_selector>`: Repeated series selector argument that selects the
  series from which to read the label values. Optional.


The `data` section of the JSON response is a list of string label values.

This example queries for all label values for the `job` label:

```bash
$ curl 'localhost:8080/api/v1/label/job/values'
{
   "status" : "success",
   "data" : [
      "node",
      "prometheus"
   ]
}
```

### Importing data in csv
The following endpoint allows to inject data points from CSV files.

```
POST /api/historian/v0/import/csv
```

The format of the http request must be `Content-Type: multipart/form-data`

URL query parameters:

- `my_csv_file` : the path of the csv files to import. Please note that each file is imported individually independently.
In other words, listing several files or making a request per file is equivalent. The settings of the request
are common to all the files listed in the request. The files must contain a header.
- `mapping.name`: the name of the column that corresponds to "name". It is the main identifier of a metric. must be a column name from the header of the attached csv files.
- `mapping.value` : the name of the column that corresponds to "value". It is the point value. The values in this column must be numeric values. must be a column name from the header of the attached csv files.
- `mapping.timestamp` : The name of the header column which corresponds to "timestamp". It is the date of the point. By default this column must contain an epoch timestamp in milliseconds (see <<import-csv-format_date,format_date>>). must be a column name from the header of the attached csv files.
- `mapping.quality` : The name of the header column which corresponds to "quality". It is the quality of the point
(quality is currently not stored in the historian - next release will provide quality). must be a column name from the header of the attached csv files.
- `mapping.tags`: The names of the header columns to be considered as a tag. Tags are concepts that describe the metric in addition to its name ("name" column). All columns not entered in the mapping will be ignored during the injection. must be a column name from the header of the attached csv files.
- `format_date` : The expected format for the values in the "timestamp" column. Must be a value in: [MILLISECONDS_EPOCH,SECONDS_EPOCH,MICROSECONDS_EPOCH,NANOSECONDS_EPOCH]. MILLISECONDS_EPOCH by default
- `timezone_date` : the timezone for the dates in the csv. must be a valid timezone. "UTC" by default
- `group_by` : This parameter is very important! If it is misused it is possible to corrupt already existing data. List all the fields that will be used to build the chunks here. By default we group the points in chunk only according to their name. However, it is also possible to group according to the value of some tags. Be aware that this will impact the way the data will be retrieved later. For example if we inject data by grouping by "name" and the tag "factory". Then we can request the values for each factory by filtering on the correct value of the factory tag (see the API to request the points). Only if thereafter someone injects other data without grouping by the tag "factory" then the data will find themselves mixed. Accepted values are "name" (whatever the mapping, use "name" and not the name of the column in the csv). Otherwise the other accepted values are the column names added as a tag.


> If there is a problem with the request we receive a 400 BAD_REQUEST response. If everything went well we receive a 201 CREATED response:

Response description :

- `tags` : array of the tags entered in the request.
- `grouped_by` : array of the fields that are used for the group by (completed in the request).
- `report` : json object, a report on the chunks that were injected.

Here is an example.

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

### Exporting data in csv


```bash
curl -X POST 'localhost:8080/api/historian/v0/export/csv' \
  --header 'Content-Type: application/json' \
  --data-raw '{
      "from": "2019-11-26T00:00:00.000Z",
      "to": "2019-11-26T23:59:59.999Z",
      "names": [
          "ack"
      ],
      "tags":{
          "metric_id" : "b04775fb-05d1-4430-82b8-ccf012ffb951"
      },
      "max_data_points": 500,
      "sampling": {
          "algorithm": "NONE",
          "bucket_size": 5
      },
      
      "return_with_quality": false
  }'
```


### Querying clusters


```bash
curl 'localhost:8080/api/historian/v0/analytics/clustering' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "names": [
      "ack"
    ],
    "day": "2019-11-28",
    "k": 5
  }'
```