---
layout: page
title: clustering timeseries
---

If you've landed here without reading [Spark API](spark-api) it's not a good idea ! 
You'll miss the key concepts and you probably don't have the data loaded.


## Loading Measures from solr Historian

We'll get our data loaded from solr. Remember that we have loaded some it monitoring data (a few days) and theer we will focus on `ack` (message acknowledgment count) and those with some real values (!=0).

```scala
// get Chunks data from solr
val acksDS = ReaderFactory.getChunksReader(ReaderType.SOLR)
  .read(sql.Options("historian", Map(
    "zkhost" -> "localhost:9983",
    "collection" -> "historian",
    "query" -> "chunk_origin:shell AND name:ack AND !chunk_avg:0.0"
  ))).as[Chunk](Encoders.bean(classOf[Chunk]))
  .cache()

// dispplay data
acksDS.select("day", "tags.host_id", "avg", "count", "start", "sax")
  .orderBy("day")
  .show(false)
```

Here we can see that the chunks have been bucketized by day for each host_id. Look how you can *sense* the shape of each timeseries by just looking at the sax string. 

```
+----------+--------------------------------+--------------------+-----+-------------+------------------------+
|day       |host_id                         |avg                 |count|start        |sax                     |
+----------+--------------------------------+--------------------+-----+-------------+------------------------+
|2019-11-25|aa27ac7bc75f3afc34849a60a9c5f62c|0.01944444444444447 |288  |1574640148000|cccccccdccccccececcccccc|
|2019-11-25|92c5261be727ebe346279cc7d0daa325|0.04236111111111115 |288  |1574640276000|cccccccccccccdedcccccccc|
|2019-11-25|0da342d5cd1c9a1f720113ad49bc6426|1624.3625           |288  |1574640176000|bcbbaabcdddccdddeedbbccc|
|2019-11-25|089ab721585b4cb9ab20aaa6a13f08ea|151.78611111111113  |288  |1574640104000|bbcaabcdddddddcdddccbbbb|
|2019-11-25|684d6b7810a15d10426af264f4704b77|148.1125            |288  |1574640030000|bbbbbcbddddddddccddabbbc|
|2019-11-25|fa66133c2fda75c946144b99d05f53b6|0.03333333333333335 |288  |1574640035000|ccccccccccccceeccccdcccc|
|2019-11-25|db7cb8cbc1a0ee4bc6b95081e6530a30|16.09236111111111   |288  |1574640020000|dcccbdccbdbdcccbdbdccdcb|
|2019-11-25|35600282722d3dd57e2e83adbcebefbf|16.945833333333333  |288  |1574640137000|ccdcbccdcbebdcbebbccdbcc|
|2019-11-25|1b07b3640a855e5b114d7b929966d9fc|0.002076124567474052|289  |1574640235000|cccccccceccccccccccccccc|
|2019-11-25|fa5cf3c715784d5df0509f5dbea7c44f|16.81875            |288  |1574640112000|ccdbdcbcdcccbdbdccccbcbc|
|2019-11-25|9bea1ec113b48a23edbf2e264c4f6aac|1642.85625          |288  |1574640109000|abbbbbbdddddddddedcbbbcc|
|2019-11-25|144e91cc7849dac578571e2229024c01|151.69791666666666  |288  |1574640105000|abcbbbcdeeddedcdcdccbbbb|
|2019-11-25|51c390afac2f78e0d48c370a991bb94e|1650.0694444444443  |288  |1574640202000|babbbbcdcdddeddeddcbbbbb|
|2019-11-26|aa27ac7bc75f3afc34849a60a9c5f62c|0.036111111111111156|288  |1574726538000|cccccccccdcccececccccccc|
|2019-11-26|db7cb8cbc1a0ee4bc6b95081e6530a30|17.53263888888889   |288  |1574726559000|bcccbcdccccccccdcdbcdcdb|
|2019-11-26|35600282722d3dd57e2e83adbcebefbf|16.990972222222222  |288  |1574726482000|ccbccccbccccccdcdccebdcb|
|2019-11-26|089ab721585b4cb9ab20aaa6a13f08ea|154.10416666666666  |288  |1574726496000|abbbbbcdedddddddddcbbbbb|
|2019-11-26|51c390afac2f78e0d48c370a991bb94e|1627.8666666666666  |288  |1574726594000|bbbbaabbedddcedeeddbbbbb|
|2019-11-26|fa5cf3c715784d5df0509f5dbea7c44f|14.774999999999999  |288  |1574726571000|cccccddcccccbccccbcbdbbd|
|2019-11-26|fa66133c2fda75c946144b99d05f53b6|0.05763888888888889 |288  |1574726425000|bbbbbbcccdccedddccbbbbbb|
+----------+--------------------------------+--------------------+-----+-------------+------------------------+
```

let's play with spark MLLib and Kmeans. 

MLlib standardizes APIs for machine learning algorithms to make it easier to combine multiple algorithms into a single pipeline, or workflow. This section covers the key concepts introduced by the Pipelines API, where the pipeline concept is mostly inspired by the scikit-learn project. If you're not familiar with those concepts it is worth reading [Spark MLLib documentation](https://spark.apache.org/docs/latest/ml-pipeline.html)

Our pipeline is quite basic :

1. tokenize our `sax` column into a `words` column with a [Tokenizer](https://spark.apache.org/docs/latest/ml-features.html#tokenizer).
2. convert token `words` column to vectors of token counts into `features` column  with a [CountVectorizer](https://spark.apache.org/docs/3.2.0/ml-features.html#countvectorizer)
3. fit and apply a [Kmeans](https://spark.apache.org/docs/3.2.0/ml-clustering.html#k-means) model 

```scala
/* Kmeans clustering*/

val tokenizer = new RegexTokenizer().setInputCol("sax").setOutputCol("words").setPattern("(?!^)")
val vectorizer = new CountVectorizer().setInputCol("words").setOutputCol("features")
val pipeline = new Pipeline().setStages(Array(tokenizer,vectorizer))

val splits = acksDS.randomSplit(Array(0.8, 0.2), 15L)
val (train, test) = (splits(0), splits(1))


val dataset = pipeline.fit(train).transform(train)
dataset.select("day","avg","tags","sax","features").show(false)
```

this should look like this 

```
+----------+---------------------+---------------------------------------------+------------------------+-------------------------------------+
|day       |avg                  |tags                                         |sax                     |features                             |
+----------+---------------------+---------------------------------------------+------------------------+-------------------------------------+
|2019-11-30|0.0041379310344827605|[host_id -> 1b07b3640a855e5b114d7b929966d9fc]|ccccccccdcccccccccccccce|(5,[0,1,3],[22.0,1.0,1.0])           |
|2019-11-28|0.0041958041958042   |[host_id -> 92c5261be727ebe346279cc7d0daa325]|cccccccdcccdcececccccccc|(5,[0,1,3],[20.0,2.0,2.0])           |
|2019-11-28|0.032867132867132894 |[host_id -> fa66133c2fda75c946144b99d05f53b6]|cccccccccdcccceccccccccc|(5,[0,1,3],[22.0,1.0,1.0])           |
|2019-11-25|0.04236111111111115  |[host_id -> 92c5261be727ebe346279cc7d0daa325]|cccccccccccccdedcccccccc|(5,[0,1,3],[21.0,2.0,1.0])           |
|2019-11-30|3.337190082644628    |[host_id -> 2dd04973afa59f00428d048b37ae0607]|bccbbdcccdcccbccccdcbddb|(5,[0,1,2],[13.0,5.0,6.0])           |
|2019-11-29|3.3618055555555557   |[host_id -> 2ba5912d8af907fd566ec6addf034665]|cccbcbcccccdbccdccddccbc|(5,[0,1,2],[16.0,4.0,4.0])           |
|2019-11-30|3.5644599303135887   |[host_id -> 2ba5912d8af907fd566ec6addf034665]|dcbcccbccccccccccccbcedd|(5,[0,1,2,3],[17.0,3.0,3.0,1.0])     |
|2019-11-26|14.774999999999999   |[host_id -> fa5cf3c715784d5df0509f5dbea7c44f]|cccccddcccccbccccbcbdbbd|(5,[0,1,2],[15.0,4.0,5.0])           |
|2019-11-28|15.894814814814815   |[host_id -> fa5cf3c715784d5df0509f5dbea7c44f]|cdbccbbcddcccbcbcccccdcd|(5,[0,1,2],[14.0,5.0,5.0])           |
|2019-11-27|16.51388888888889    |[host_id -> db7cb8cbc1a0ee4bc6b95081e6530a30]|dbcdbdbcdbdbdbccbdbbdcdb|(5,[0,1,2],[5.0,9.0,10.0])           |
|2019-11-25|16.81875             |[host_id -> fa5cf3c715784d5df0509f5dbea7c44f]|ccdbdcbcdcccbdbdccccbcbc|(5,[0,1,2],[13.0,5.0,6.0])           |
|2019-11-28|29.29020979020984    |[host_id -> f816c80fff09c542894caabe5524aa3c]|cccccccccccccceecccccccc|(5,[0,3],[22.0,2.0])                 |
|2019-11-28|46.99722222222219    |[host_id -> 2ba5912d8af907fd566ec6addf034665]|cccccccccccccceddccccccc|(5,[0,1,3],[21.0,2.0,1.0])           |
|2019-11-27|143.87762237762237   |[host_id -> 144e91cc7849dac578571e2229024c01]|cbccbbccdddddddddddcccca|(5,[0,1,2,4],[9.0,11.0,3.0,1.0])     |
|2019-11-26|146.58541666666667   |[host_id -> 684d6b7810a15d10426af264f4704b77]|bcbbcbcdedcdbcdddddbbbba|(5,[0,1,2,3,4],[5.0,8.0,9.0,1.0,1.0])|
|2019-11-25|148.1125             |[host_id -> 684d6b7810a15d10426af264f4704b77]|bbbbbcbddddddddccddabbbc|(5,[0,1,2,4],[4.0,10.0,9.0,1.0])     |
|2019-11-25|151.78611111111113   |[host_id -> 089ab721585b4cb9ab20aaa6a13f08ea]|bbcaabcdddddddcdddccbbbb|(5,[0,1,2,4],[5.0,10.0,7.0,2.0])     |
|2019-11-27|1594.3713286713287   |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|cbccbcccdddddddddddcccca|(5,[0,1,2,4],[10.0,11.0,2.0,1.0])    |
|2019-11-27|1619.5358885017422   |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|bccccccddddddddddddcccca|(5,[0,1,2,4],[10.0,12.0,1.0,1.0])    |
|2019-11-25|1624.3625            |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|bcbbaabcdddccdddeedbbccc|(5,[0,1,2,3,4],[7.0,7.0,6.0,2.0,2.0])|
+----------+---------------------+---------------------------------------------+------------------------+-------------------------------------+

```

now we will fit a [KMeans](https://spark.apache.org/docs/3.2.0/ml-clustering.html#k-means) clustering model and transform the initial dataset to see where our timeseries are clustered

```scala
val kmeans = new KMeans().setK(2).setSeed(1L).setMaxIter(20)
val model = kmeans.fit(dataset)
val predictions = model.transform(dataset)
predictions.select("day", "avg","tags","sax","prediction").orderBy("day","prediction").show(300,false)
model.clusterCenters.foreach(println)
```

It's not deep acurate data science here. But we can already see a few interesting things. 
If we get interested in what do change from day to day, we could just try to arrange metrics into 2 clusters.
And for some metrics it's really clear that one day is different from the others :
something has changed the `2019-11-28` for at least host_id=`51c390afac2f78e0d48c370a991bb94e, 0da342d5cd1c9a1f720113ad49bc6426, 9bea1ec113b48a23edbf2e264c4f6aac`

```markdown
+----------+---------------------+---------------------------------------------+------------------------+----------+
|day       |avg                  |tags                                         |sax                     |prediction|
+----------+---------------------+---------------------------------------------+------------------------+----------+
|2019-11-25|1650.0694444444443   |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|babbbbcdcdddeddeddcbbbbb|0         |
|2019-11-26|1627.8666666666666   |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|bbbbaabbedddcedeeddbbbbb|0         |
|2019-11-27|1619.5358885017422   |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|bccccccddddddddddddcccca|0         |
|2019-11-28|1683.4397212543554   |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|bbbbbbbbcdeccccccccccccc|`1`       |
|2019-11-29|1629.0125            |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|abbbbabcccdedddddedcbcbb|0         |
|2019-11-30|1617.7074380165288   |[host_id -> 51c390afac2f78e0d48c370a991bb94e]|abcbbbbbbcddccddddddddcb|0         |

|2019-11-25|1624.3625            |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|bcbbaabcdddccdddeedbbccc|0         |
|2019-11-26|1644.451388888889    |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|babbbbcdeeddcdcddccbbbbb|0         |
|2019-11-27|1604.547038327526    |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|bccbbccddddddddddddcccca|0         |
|2019-11-28|1693.355944055944    |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|bbbbbbbbcdeccccccccccccc|`1`       |
|2019-11-29|1620.0375            |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|abbbbbccededdddeeddabbab|0         |
|2019-11-30|1638.844599303136    |[host_id -> 0da342d5cd1c9a1f720113ad49bc6426]|cabbbbbcdedddddddddbbbab|0         |

|2019-11-25|1642.85625           |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|abbbbbbdddddddddedcbbbcc|0         |
|2019-11-26|1680.5118055555556   |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|bcbbbbbdeeddeddddccbbaaa|0         |
|2019-11-27|1594.3713286713287   |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|cbccbcccdddddddddddcccca|0         |
|2019-11-28|1610.2126315789476   |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|bbbbbbbbcdeccccccccccccc|`1`       |
|2019-11-29|1652.8368055555557   |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|aaabbacdeeeeeedddddbbaba|0         |
|2019-11-30|1621.837630662021    |[host_id -> 9bea1ec113b48a23edbf2e264c4f6aac]|abbabbcddedeedccdddbabbb|0         |

+----------+---------------------+---------------------------------------------+------------------------+----------+
```


## What's next
Now we have a basic understanding of Spark API, you may ask where to go from there ?

- Have a look to dataviz with [Grafana Prometheus](data-viz)
- See how to do realtime interactions through [REST API](rest-api)

