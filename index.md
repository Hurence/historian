
## Welcome to Hurence Data Historian


Hurence has developed an open source Data Historian. It is Big Data and can archive your time series (sensor data) without volume limitation with the performance of a simple to operate Big Data infrastructure and features worthy of historical data historians, and much more ...



# Features
Here is a non exhaustive list of features

- **Search engine on time-stamped data** : Searching over time is a key feature of a data historian. We have therefore chosen to build our data historian on the Big Data SolR search engine, but have taken care to be able to support other engines such as Elasticsearch or OpenDistro.

- **Searching for data with words like on Google** : With our symbolic data encoding (SAX) feature, it is possible to search for data on "words" as you would on Google. Letters are automatically associated with values and sequences of values form words that can be queried.

- **Anomaly detection and alerting** : SAX encoding enables the detection of anomaly patterns in an innovative way.

- **Sampling integrated into visualization** : Viewing all of the points over a long period is possible thanks to sampling algorithms. They preserve the visual aspect of curves very well.

- **Simple single node installation** : A move to a multi-node Big Data scale and secure data

- **Native cross-data center scalability & synchronization** : We use the native ability of SolR Cloud to scale horizontally by adding servers in “cluster” mode of machines. SolR also gives us the ability to synchronize clusters between data centers.