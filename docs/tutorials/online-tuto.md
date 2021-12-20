

## Detailed story board and script


### Script 1 : presentation

Hi, I'm Thomas R&D director in Hurence, a big data experts company in the french Alps.
I've designed Hurence Data Historian a few years ago based on a fork of ChronixDB.

(a link to chronix)

Data Historians are an old piece of software in industrial systems, but what makes Hurence Data Historian different
is that it has evolved from being just a place for storing data to becoming a whole data infrastructure
dedicated to Industry 4.0 with the capability of :

(a problem to solve slide here)

- providing low-cost and long-term easily searchable storage
- handling parallel AI workloads on massive datasets
- Pattern recognition and anomaly detection
- being cloud native support or lightweight edge deployment

Let's now talk about the core underlying concepts for Hurence Data Historian chunk oriented storage :

(concept slides here)

- Time compaction : the compaction ratios come from storing only offsets in timestamps and (optionally) removing values below a timestamp delta threshold.
- Blob encoding : the searchable ability and the long term capacity resides on the encoding of a set of related Measure(s) (uniquely identified by a name and a set of key/value tags) within a time bucket into a single binary Chunk o data.
- SAX encoding : a method used to simplify time series through a kind of summary of time intervals, the data becomes much smaller and easier to process, while still capturing its important aspects.

We are now ready to talk about the software components of the data historian

(components slides here)

Distributed search engine backend as infrastructure.
Timeseries manipulation base library for low level access.
REST API that handles live interactions.
Big data API to manipulate chunks and measures through a parallelized framework and bundled tools for batch processing (compaction, batch loading, ML analytics).
High level dataviz and exploration

All these components can be deployed in two flavors (as lightweight single node aka edge mode or in kubernetes

(infrastructure)

Kubernetes, is an open-source system for automating deployment, scaling, and management of containerized applications.
Solr is the popular, blazing-fast, open source enterprise search platform built on Apache Lucene™.
Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

How to get data in and out ?

(data access slide)

- graph level
- rest api through PromQL dialect
- SQL level

### Script 2 : installation




## Benefits highlights


## Combine audio and
