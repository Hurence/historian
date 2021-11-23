---
layout: page
title: Features
---

Here is a non exhaustive list of features

- **Search engine on chronogical data** : Searching over time is a key feature of a data historian. We have therefore chosen to build our data historian on the Big Data SolR search engine, but have taken care to be able to support other engines such as Elasticsearch or OpenDistro.

- **Searching for data with words like on Google** : With symbolic data encoding or [SAX representation](sax-encoding), it is possible to search for data on "words" as you would on Google. Letters are automatically associated with values and sequences of values form words that can be queried ; this can be viewed as the DNA of time series.

- **AI Powered at scale** : Apache Spark (and MLLib) API will handle many kind of massively parallel workloads. For instance you can load timeseries data (csv like) from any kind of distributed filesystem (s3, aks, hdfs, ...) or database (cassandra, mongo, solr, ...) into Smart Historian. You can also do unsupervised clustering on timeseries (through SAX sring for example). Or simply forecasting to create alerts on business rules.

- **Pattern recognition and Anomaly detection** :  [SAX encoding](sax-encoding) enables the detection of anomaly patterns by using a time series representation designed to vastly reduce the data dimensionality and redundancy by subsampling and quantization. The “obvious” way to detect an
  anomaly is to search for the subsequence (in SAX string) with the highest distance to any other subsequence.

- **Sampling integrated into visualization** : Viewing all of the points over a long period is possible thanks to sampling algorithms. They preserve the visual aspect of curves very well.

- **Cloud native support or single node installation** : Deploy your infrastructure over Kubernetes is a breeze thanks to Helm and Kubernetes. You can also easily deploy things as a single node application in case of edge deployment.

- **Support to open-source connectivity** : A wide range of data access technology support needed like OPC DA, HDA & UA, APIs, SQL, programmatic access via Software Development Kit (SDK), support to Microsoft's Component Object Model (COM), Data connectors, Web API interfacing, etc.

- **Speedy ingress and egress** : Fast access for real-time analytics, machine learning, and AI as a One-Stop shop.

- **High availability and redundancy** : Features that can mirrors the data across multiple nodes in the industry with high availability at each level and techniques. We use the native ability of SolR Cloud to scale horizontally by adding servers in *cluster* mode of machines. SolR also gives us the ability to synchronize clusters between data centers.
  
- **Visualization and alert** : High-quality querying, visualizing, and notifying and alerting capability.
Data enrichment and cleansing capability: Inbuilt data aggregation, auto data cleansing, interpolation, and data enrichment capability.
