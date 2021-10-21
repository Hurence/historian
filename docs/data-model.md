---
layout: page
title: Data Model
---

This section describes protocol buffers specification and solr schema for storage.

### Protocol Buffers

The following protocol [buffer specification](https://github.com/Hurence/historian/blob/master/historian-timeseries/src/main/protobuf/chunk_protocol_buffers.proto) is the art of the serialized value of a Chunk

```proto
option java_package = "com.hurence.timeseries.converter.serializer";

option optimize_for = SPEED;

//Our point
message Point {
    //The delta as long
    optional uint64 tlong = 1;
    //Perhaps we can store the delta as int
    optional uint32 tint = 2;
    //Value
    optional double v = 3;
    //Or the index of the value
    optional uint32 vIndex = 4;
    //timestamp base deltas
    optional uint64 tlongBP = 5;
    optional uint32 tintBP = 6;
}

//The data of a time series is a list of points
message Chunk {
    // The list of points
    repeated Point p = 1;
    // the used ddc threshold
    optional uint32 ddc = 2;
    // The list of qualities, only saving for each change
    repeated Quality q = 3;
    // metric name
    optional string name = 4;
    // UTC timestamp for start time of the chunk (in ms)
    optional uint64 start = 5;
    // UTC timestamp for end time of the chunk (in ms)
    optional uint64 end = 6;   
}

message Quality {
    // the index of the first point with this quality, 
    // not given for first point
    // we will store it each time the quality changes
    optional uint32 pointIndex = 1;
    // the Value of the quality
    optional float v = 2;
    // Or the index of the value if it is already stocked
    optional uint32 vIndex = 3;
}

```


### SolR schema

In SolR the chunks will be stored according to the following schema (for the current version). Additional field will be stored as tag names.

```xml
<schema name="default-config" version="1.6">
    ...
    <field name="chunk_avg" type="pdouble"/>
    <field name="chunk_count" type="pint"/>
    <field name="chunk_day" type="string"/>
    <field name="chunk_end" type="plong"/>
    <field name="chunk_first" type="pdouble"/>
    <field name="chunk_hour" type="pint"/>
    <field name="chunk_last" type="pdouble"/>
    <field name="chunk_max" type="pdouble"/>
    <field name="chunk_min" type="pdouble"/>
    <field name="chunk_month" type="pint"/>
    <field name="chunk_origin" type="string"/>
    <field name="chunk_outlier" type="boolean"/>
    <field name="chunk_quality_avg" type="pfloat"/>
    <field name="chunk_quality_first" type="pfloat"/>
    <field name="chunk_quality_max" type="pfloat"/>
    <field name="chunk_quality_min" type="pfloat"/>
    <field name="chunk_quality_sum" type="pfloat"/>
    <field name="chunk_sax" type="ngramtext"/>
    <field name="chunk_start" type="plong"/>
    <field name="chunk_std_dev" type="pdouble"/>
    <field name="chunk_sum" type="pdouble"/>
    <field name="chunk_trend" type="boolean"/>
    <field name="chunk_value" type="text_general" multiValued="false" indexed="false"/>
    <field name="chunk_year" type="pint"/>
    <field name="metric_id" type="string" docValues="true" multiValued="false" indexed="true" stored="true"/>
    <field name="metric_key" type="string"/>
    <field name="name" type="string" multiValued="false" indexed="true" required="true" stored="true"/>
</schema>
```