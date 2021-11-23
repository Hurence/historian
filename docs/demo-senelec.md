


curl -g "http://localhost:8983/solr/historian/update" \
-H 'Content-Type: application/json' \
-d '{"delete":{"query":"chunk_origin:senelec AND chunk_avg:0.0"}}'

## Direct Access to solr

Search for a SAX pattern 

`http://10.20.20.97:8983/solr/#/historian/query?q=*:*&q.op=OR&fl=metric_key,%20chunk_sax&fq=chunk_origin:senelec&fq=chunk_sax:ddcbbbbbbbbbbbbbaaa&fq=chunk_day:%5B2021-08-08%20TO%202021-08-09%5D`


```json
{
    "chunk_trend":true,
    "chunk_origin":"senelec",
    "chunk_count":144,
    "chunk_outlier":false,
    "chunk_value":"H4sIAAAAAAAAAH1SOywEURSdHYKZICQUo+AVCkJkEwoRMhERGmKpxGqVG6KjodhGo6alluhIhPWpxP+v8bfZZHZlFdTuuTNzn4aXnLx77zvv/m3LeZ6pWut7fnHtImVEU6f1tqkMhuUs7n01xBfpyVSFAsvp5y/nLolTXfbtyuQxxMY46Ecg26Ru1Ca92MYJ1BJSVyfBPINaSrD+gf0Hwne4M/iIOzwrQjk/a1toV1LMBVJt5XMJkbKnxFic4HMDMRtD+ueau49IUYHlzKJpMwcwtwnCqre1j3WIy0s4myC3Sy9SUDsJzWT6bkD3nkAeZh93EPeR8d61K/GuINYhRC1bfU/8jbsff4TTXkaYzENocgWWc3yE86YL9CCy46Snm/HhSm55fI1JzDz2ZSSaWi61yyrmvKJgEz5dqIeVwf80sSpop9SA7Mq7Dp/VG5RzZah5XW5eczn8CCFOGBPyr4496U7fgzzE+eJTuLGfMI9zx03VFNwdjHB9t1wxmSoRzMuf5K4/Pt+cCOTpYMG6CT2ElqD1/lq96g3z/JQshwurosYWqPmIzDiH51FJI+urBSoZYcFUg1JnRu/AO2gDMum0nlYG/hciNcUlRrVhzO38AJQoC4zoAwAA",
    "chunk_start":1628467200000,
    "chunk_quality_max":"NaN",
    "id":"1ea2e242a1a6b73e78fc7bfce1e16e0782b255cc2720f150e8b3fd5881b5a2a3",
    "chunk_max":1.09,
    "version":"VERSION_1",
    "chunk_std_dev":0.31784234582376963,
    "chunk_quality_min":"NaN",
    "chunk_min":0.07,
    "chunk_first":0.59,
    "metric_key":"P8393|Meter_No$30021634",
    "chunk_quality_sum":"NaN",
    "chunk_month":8,
    "chunk_sum":67.33999999999999,
    "chunk_sax":"ddcbbbbbbbbbbbbbaaaaaccccdeeeeeedeebaaaabceeeede",
    "chunk_end":1628551800000,
    "chunk_avg":0.4676388888888889,
    "chunk_last":0.76,
    "chunk_day":"2021-08-09",
    "chunk_year":2021,
    "chunk_quality_avg":"NaN",
    "name":"P8393",
    "chunk_quality_first":"NaN",
    "Meter_No":"30021634",
    "_version_":1716069375846383617
}
```


## Basic exploratory analysis

```scala
val ds = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
    .csv(path)
    .drop("PE401", "PE402", "PE403", "PE601", "PE602", "PE603", "P8332", "P8333")
    .cache()



ds.select(count(ds("P8333").isNull)).show
+----------------------+                                                        
|count((P8333 IS NULL))|
+----------------------+
|              21448851|
+----------------------+


val rows_count = ds.count
// rows_count: Long = 21448851  

ds.columns.map( c => (c,ds.filter(ds(c).isNull).count / rows_count.toFloat))
Array((ID_V_LoadProfile_Min,0.0), (Data_tstmp,0.0), (Meter_No,0.0), (PE300,0.10225382), (PE400,0.4217684), (PE500,0.102186635), (PE600,0.4217684), (PE301,0.68048537), (PE302,0.68057585), (PE303,0.6804161), (PE501,0.6804119), (PE502,0.6804161), (PE503,0.680414), (P8311,0.6575879), (P8312,0.6575879), (P8313,0.6575879), (P8391,0.24741432), (P8392,0.26256347), (P8393,0.26256347), (P8023,0.7874283), (P8341,0.45998597), (P8342,0.45998597), (P8331,0.7874283), (TauxDeCharge,0.72378445), (S_Approximee,0.6575879), (Q_Approximee,0.69144577), (P8311_S,0.6575879), (P8312_S,0.6575879), (P8313_S,0.6575879), (CosPhi,0.6634446), (ID_Saison,0.99999994), (ID_Typeintervalle,0.99999994), (ID_TypeJour,0.99999994))

```






## Data import


```scala
import com.hurence.historian.service.SolrChunkService
import com.hurence.historian.spark.ml.{Chunkyfier, ChunkyfierStreaming,UnChunkyfier}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader._
import com.hurence.historian.spark.sql.writer.solr.SolrChunkForeachWriter
import com.hurence.historian.spark.sql.writer._
import com.hurence.timeseries.model.{Measure,Chunk}
import com.hurence.historian.spark.sql
import com.hurence.historian.spark.sql.reader._
import com.hurence.historian.spark.sql.writer.solr.SolrChunkForeachWriter
import com.hurence.historian.spark.sql.writer._
import com.hurence.timeseries.model.{Measure,Chunk}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.ml.feature.{VectorAssembler,NGram,RegexTokenizer, Tokenizer, CountVectorizer}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

val path = "exporthisto_vloadprofil.csv"

// select only interesting columns
val colNames = List("PE300", "PE400", "PE500", "PE600", "PE301", "PE302", "PE303", "PE501", "PE502", "PE503", "P8311", "P8312", "P8313", "P8391", "P8392", "P8393", "P8023", "P8341", "P8342", "P8331" /*, "TauxDeCharge", "S_Approximee", "Q_Approximee", "P8311_S", "P8312_S", "P8313_S", "CosPhi"*/)

def inject(path:String, metricName:String) = {
    val measuresDS = ReaderFactory.getMeasuresReader(ReaderType.CSV)
        .read(sql.Options(
            path,
            Map(
                "inferSchema" -> "true",
                "delimiter" -> ";",
                "header" -> "true",
                "defaultName" -> metricName,
                "nameField" -> "",
                "timestampField" -> "Data_tstmp",
                "timestampDateFormat" -> "yyyy-MM-dd HH:mm:ss",
                "valueField" -> metricName,
                "qualityField" -> "",
                "tagsFields" -> "Meter_No"
            ))).cache()


    // setup the chunkyfier
    val chunkyfier = new Chunkyfier()
        .setOrigin("senelec")
        .setDateBucketFormat("yyyy-MM-dd")
        .setSaxAlphabetSize(5)
        .setSaxStringLength(48)
    
    // transform Measures into Chunks
    val chunksDS = chunkyfier.transform(measuresDS)
        .as[Chunk](Encoders.bean(classOf[Chunk]))
        .cache()
    
    // write to solr
    WriterFactory.getChunksWriter(WriterType.SOLR)
        .write(sql.Options("historian", Map(
        "zkhost" -> "localhost:9983",
        "collection" -> "historian"
        )), chunksDS)
}


// inject them one by one
colNames.foreach( c => inject(path, c))
```




## Kmeans clustering

```scala

def cluster(startDay:String, endDay:String, name:String, meterNo:String, k:Int, n:Int ) = {
      val senelecDs = ReaderFactory.getChunksReader(ReaderType.SOLR)
        .read(sql.Options("historian", Map(
          "zkhost" -> "localhost:9983",
          "collection" -> "historian",
          "query" -> s"chunk_origin:senelec AND name:$name AND chunk_day:[$startDay TO $endDay] AND Meter_No:$meterNo"
        ))).as[Chunk](Encoders.bean(classOf[Chunk]))
        .cache()
    
      // display data
      senelecDs.select("day", "tags.Meter_No", "avg", "count", "start", "sax")
        .orderBy("day")
        .show(false)


  
      val tokenizer = new RegexTokenizer().setInputCol("sax").setOutputCol("words").setPattern("(?!^)")
  val ngram = new NGram().setN(n).setInputCol("words").setOutputCol("ngrams")
      val vectorizer = new CountVectorizer().setInputCol("ngrams").setOutputCol("features")
      val pipeline = new Pipeline().setStages(Array(tokenizer,ngram,vectorizer))
    
     /* val splits = senelecDs.randomSplit(Array(0.8, 0.2), 15L)
      val (train, test) = (splits(0), splits(1))
    */
    
      val dataset = pipeline.fit(senelecDs).transform(senelecDs)
      dataset.select("day","avg","tags","sax","features").show(false)
    
    
      val kmeans = new KMeans().setK(k).setSeed(1L).setMaxIter(50)
      val model = kmeans.fit(dataset)
      val predictions = model.transform(dataset)
      predictions.select("day", "avg","tags","sax","prediction").orderBy("day","prediction").show(3000,false)
     // model.clusterCenters.foreach(println)
}

cluster("2021-08-11", "2021-08-18", "PE300", "30004466", 3, 10)

```






## SAX similarity

chunk_sax:bbbbbbbbbbbbbaaaaacc
for day 2021-08-08
P8393{Meter_No="30021634"}
P8392{Meter_No="30103291"}


chunk_sax:ccccceecccccc
for day 2021-08-10
PE500{Meter_No="30009151"}
PE500{Meter_No="30014460"}