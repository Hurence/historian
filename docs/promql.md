---
layout: page
title: PromQL Cheat Sheet
---

Prometheus provides a functional query language called PromQL (Prometheus Query Language) that lets the user select and aggregate time series data in real time. The result of an expression can either be shown as a graph, viewed as tabular data in Prometheus's expression browser, or consumed by external systems via the HTTP API.


## Time series dimensions and labels

Another feature of a TSDB is the ability to filter measurements using tags. Each data point is labeled with a tag that adds context information, such as where the measurement was taken.

With time series data, the data often contain more than a single series, and is a set of multiple time series. Many Grafana data sources support this type of data.

The common case is issuing a single query for a measurement with one or more additional properties as dimensions. For example, querying a temperature measurement along with a location property. In this case, multiple series are returned back from that single query and each series has unique location as a dimension.

To identify unique series within a set of time series, Grafana stores dimensions in labels.

Each time series in Grafana optionally has labels. labels are set a of key/value pairs for identifying dimensions. Example labels could are `{location=us}` or `{country=us,state=ma,city=boston}`. Within a set of time series, the combination of its name and labels identifies each series. For example, temperature `{country=us,state=ma,city=boston}`.

Different sources of time series data have dimensions stored natively, or common storage patterns that allow the data to be extracted into dimensions.

Time series databases (TSDBs) usually natively support dimensionality. Prometheus also stores dimensions in labels. In TSDBs such as Graphite or OpenTSDB the term tags is used instead.

In table databases such SQL, these dimensions are generally the GROUP BY parameters of a query

## Expression language data types
In Prometheus's expression language, an expression or sub-expression can evaluate to one of four types:

- `Instant vector` - a set of time series containing a single sample for each time series, all sharing the same timestamp
- `Range vector` - a set of time series containing a range of data points over time for each time series
- `Scalar` - a simple numeric floating point value
- `String` - a simple string value; currently unused

Depending on the use-case (e.g. when graphing vs. displaying the output of an expression), only some of these types are legal as the result from a user-specified expression. For example, an expression that returns an instant vector is the only type that can be directly graphed.


## Historian mapping
Hurence historian implements a subset of this query language to interact with time series:

| name                         | metric        | unit | sub_unit    | type                |
| ---------------------------- |:-------------:| ----:| ----------- | ------------------- |
| UT900Z.U028_COEF_A_FI01.F_CV | col_flow_rate | u028 | h2_reactor1 | coefficient_metro_a |
| UT900Z.U028_COEF_B_FI01.F_CV | col_flow_rate | u028 | h2_reactor1 | coefficient_metro_b |
| UT900Z.U028_COEF_A_FI02.F_CV | col_flow_rate | u028 | h2_reactor2 | coefficient_metro_a |


T473.SC02_OP.F_CV{ sampling_algo="min", bucket_size="10" }



## Selecting series
Select latest sample for series with a given metric (Instant vector selectors) :

`col_flow_rate`

Select 5-minute range of samples for series with a given metric name (Range Vector Selectors):

`col_flow_rate[5m]`

Only series with given label values:

`col_flow_rate{unit="u028", type="coefficient_metro_a", quality="true", sampling="" }`

Complex label matchers:

`col_flow_rate{unit!="u028", type=~"coefficient_metro_a|coefficient_metro_b"}`


- `=`  : Equality
- `!=` : Non-equality
- `=~` : Regex match
- `!~` : Negative regex match


## Rates of increase for counters
Per-second rate of increase, averaged over last 5 minutes:

``rate(col_flow_rate[5m])`

Per-second rate of increase, calculated over last two samples in a 1-minute time window:

`irate(col_flow_rate[1m])`

Absolute increase over last hour:

`increase(col_flow_rate[1h])`

## Aggregating over multiple series
Sum over all series:

`sum(col_flow_rate)`

Preserve the instance and job label dimensions:

`sum by(job, instance) (col_flow_rate)`

Aggregate away the instance and job label dimensions:

`sum without(instance, job) (col_flow_rate)`

Available aggregation operators:

- `sum()`
- `min()`
- `max()`
- `avg()`
- `stddev()`
- `stdvar()`
- `count()`
- `count_values()`
- `group()`
- `bottomk()`
- `topk()`
- `quantile()`

## Filtering series by value
Only keep series with a sample value greater than a given number:

`node_filesystem_avail_bytes > 10*1024*1024`

Only keep series from the left-hand side whose sample values are larger than their right-hand-side matches:

`go_goroutines > go_threads`

Instead of filtering, return 0 or 1 for each compared series:

`go_goroutines > bool go_threads`

Match only on specific labels:

`go_goroutines > bool on(job, instance) go_threads`

Available comparison operators: `==, `!=, `>, `<, `>=`, `<=`