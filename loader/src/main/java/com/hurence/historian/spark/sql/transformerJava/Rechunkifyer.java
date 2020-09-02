package com.hurence.historian.spark.sql.transformerJava;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.timeseries.modele.chunk.ChunkVersion0;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0.*;
import static com.sun.codemodel.internal.JExpr.lit;
import static jdk.nashorn.internal.objects.NativeArray.concat;


public class Rechunkifyer implements Transformer<RechunkOptions, ChunkVersion0, ChunkVersion0> {

    @Override
    public Dataset<ChunkVersion0> transform(RechunkOptions opt, Dataset<ChunkVersion0> ds) {
        Integer CHUNK_NEW_SIZE =  opt.OUTPUT_CHUNK_SIZE;

//        JavaRDD<ChunkVersion0> data = ds.javaRDD();
//        Tuple2<String,String> groupByCols = new Tuple2<>(HistorianChunkCollectionFieldsVersionEVOA0.TAGNAME,HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY);
//
//        JavaPairRDD<Object, Iterable<ChunkVersion0>> groupedByChunks = data.groupBy(r ->);
//        JavaPairRDD<String, List<ChunkVersion0>> grouped = null;


//        Dataset<Row> data = ds.groupBy(HistorianChunkCollectionFieldsVersionEVOA0.TAGNAME, HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY)
//                                           .agg(HistorianChunkCollectionFieldsVersionEVOA0.NAME);

//        Dataset<ChunkVersion0> ds1 = ds.withColumn("new col", concat(), lit(","), concat(TAGNAME));


        return null;
    }

    public  Dataset<ChunkVersion0>  transformListOfChunksToDatasetOfChunks (ArrayList<ChunkVersion0> data){
        SparkConf conf = new SparkConf().setAppName("SparkSample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqc = new SQLContext(jsc);

        Dataset <Row> dtaFrameOfChunks = sqc.createDataFrame(data, ChunkVersion0.class);
        Dataset<ChunkVersion0> dataSetOfChunks = dtaFrameOfChunks.as(Encoders.bean(ChunkVersion0.class));

        return dataSetOfChunks;
    }

}
