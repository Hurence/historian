---
layout: page
title: Concepts
---

This is a free solution to handle massive loads of timeseries data into a search engine (such as Apache SolR).
The key concepts are simple :

- **Measure** is a point in time with a floating point value identified by a name and some tags (categorical features)
- **Chunk** is a set of contiguous Measures with a time interval grouped by a date bucket, the measure name and eventually some tags

The main purpose of this tool is to help creating, storing and retrieving these chunks of timeseries.
We use chunking instead of raw storage in order to save some disk space and reduce costs at scale. Also chunking is very usefull to pre-compute some aggregation and to facilitate down-sampling

### Measure

A `Measure` point is identified by the following fields. Tags are used to add meta-information.
```java
class Measure  {
    String name;
    long timestamp;
    double value;
    float quality;
    Map<String, String> tags;
}
```

### Chunk

A `Chunk` is identified by the following fields
```java
class Chunk  {
SchemaVersion version = SchemaVersion.VERSION_1;
String name, id, metricKey;
byte[] value;
long start, end;
Map<String, String> tags;

    long count, first, min, max, sum, avg, last, stdDev;
    float qualityFirst, qualityMin, qualityMax, qualitySum, qualityAvg;

    int year, month;
    String day;
    String origin;
    String sax;
    boolean trend;
    boolean outlier;
}
```

As you can see from a `Measure` points to a `Chunk` of Measures, the `timestamp` field has been replaced by a `start` and `stop` interval and the `value` is now a base64 encoded string named `chunk`.



<div class="card text-white bg-dark mb-12" >
  <div class="card-header">Chunk encode value</div>
  <div class="card-body">
    <p class="card-text">The chunk value is a protocol buffer encoded value according to the protobuf  specification], please refer to <a href="data-model">data model section</a> to learn more about that.</p>
  </div>
</div>

### SAX symbolic encoding

Note that the Chunk has aggregated fields. In addition to the classic statistics on the value and the quality of the point, we also integrate symbolic encoding with SAX.

The advantage of using SAX is that it is able to act as a dimensionality reduction tool, it tolerates time series of different lengths and makes it easier to find trends.

SAX encoding is a method used to simplify time series through a kind of summary of time intervals. By averaging, grouping and symbolically representing periods, the data becomes much smaller and easier to process, while still capturing its important aspects. For example, it can be used to detect statistical changes in trends and therefore abnormal behavior.


- [https://www.kdnuggets.com/2019/09/time-series-baseball.html](https://www.kdnuggets.com/2019/09/time-series-baseball.html)
- [http://www.marc-boulle.fr/publications/BonduEtAlIJCNN13.pdf](http://www.marc-boulle.fr/publications/BonduEtAlIJCNN13.pdf)
