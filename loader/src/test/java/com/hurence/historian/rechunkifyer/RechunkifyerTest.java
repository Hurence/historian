package com.hurence.historian.rechunkifyer;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.converter.PointsToChunkVersion0;
import com.hurence.timeseries.modele.chunk.ChunkVersion0;
import com.hurence.timeseries.modele.chunk.ChunkVersion0Impl;
import com.hurence.timeseries.modele.chunk.ChunkVersion0WithoutSchemaVersionImp;
import com.hurence.timeseries.modele.points.Point;
import jdk.nashorn.internal.objects.NativeArray;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0.*;
import static com.sun.codemodel.internal.JExpr.lit;
import static jdk.nashorn.internal.objects.NativeArray.concat;
import static org.apache.spark.sql.functions.*;


public class RechunkifyerTest implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(RechunkifyerTest.class);

    @Test
    public void testDF() {
        Map<String, String> tags = new HashMap<String, String>() {{
            put("couNtry", "France");
            put("usine", "usine 2 ;;Alpha go");
        }};

        byte[] bytes = new byte[] { (byte)0xe0, 0x4f, (byte)0xd0,
                0x20, (byte)0xea};

        ChunkVersion0WithoutSchemaVersionImp chunk = new ChunkVersion0WithoutSchemaVersionImp.Builder()
                .setAvg(1.4)
                .setChunkOrigin("test")
                .setCompactionRunnings(Arrays.asList("sup1", "sup2", "sup3"))
                .setCount(23)
                .setDay("22/09/1994")
                .setEnd(154645212)
                .setFirst(15487458)
                .setId("metric_mejd")
                .setLast(3664588)
                .setMax(659.235)
                .setMin(1.3656)
                .setMonth(9)
                .setName("Mejdeddine")
                .setSax("saxsaxsax")
                .setOutlier(false)
                .setStart(403847457)
                .setStd(34584)
                .setSum(696595864)
                .setTags(tags)
                .setTrend(false)
                .setYear(1994)
                .setValueBinaries(bytes)
                .build();



        ArrayList<ChunkVersion0WithoutSchemaVersionImp> listChunk = new ArrayList<>();
        listChunk.add(chunk);

//        implicit def myDataEncoder: org.apache.spark.sql.Encoder[Birthday] = org.apache.spark.sql.Encoders.kryo[Birthday]
//        listChunk.get(0).getVersion() = listChunk.get(0).getVersion().toString();


        SparkConf conf = new SparkConf().setAppName("SparkSample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqc = new SQLContext(jsc);

        Dataset<Row> data = sqc.createDataFrame(listChunk, ChunkVersion0WithoutSchemaVersionImp.class);
        data.show();





        //JavaRDD<ChunkVersion0> rdd = jsc.parallelize(listChunk);
        //rdd.foreach(System.out::println);

//      List<ChunkVersion0> r = rdd.collect();
//        r.get(0).getId();

    }

    @Test
    public void testDSOfChunk() {
        Map<String, String> tags = new HashMap<String, String>() {{
            put("couNtry", "France");
            put("usine", "usine 2 ;;Alpha go");
        }};

        byte[] bytes = new byte[] { (byte)0xe0, 0x4f, (byte)0xd0,
                0x20, (byte)0xea};

        ChunkVersion0WithoutSchemaVersionImp chunk = new ChunkVersion0WithoutSchemaVersionImp.Builder()
                .setAvg(1.4)
                .setChunkOrigin("test")
                .setCompactionRunnings(Arrays.asList("sup1", "sup2", "sup3"))
                .setCount(23)
                .setDay("22/09/1994")
                .setEnd(154645212)
                .setFirst(15487458)
                .setId("metric_mejd")
                .setLast(3664588)
                .setMax(659.235)
                .setMin(1.3656)
                .setMonth(9)
                .setName("Mejdeddine")
                .setSax("saxsaxsax")
                .setOutlier(false)
                .setStart(403847457)
                .setStd(34584)
                .setSum(696595864)
                .setTags(tags)
                .setTrend(false)
                .setYear(1994)
                .setValueBinaries(bytes)
                .build();



        ArrayList<ChunkVersion0WithoutSchemaVersionImp> listChunk = new ArrayList<>();
        listChunk.add(chunk);

//        implicit def myDataEncoder: org.apache.spark.sql.Encoder[Birthday] = org.apache.spark.sql.Encoders.kryo[Birthday]
//        listChunk.get(0).getVersion() = listChunk.get(0).getVersion().toString();


        SparkConf conf = new SparkConf().setAppName("SparkSample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqc = new SQLContext(jsc);

       // Dataset<ChunkVersion0WithoutSchemaVersionImp> data = sqc.createDataset(listChunk, Encoders.bean(ChunkVersion0WithoutSchemaVersionImp.class));
         Dataset<Row> data = sqc.createDataFrame(listChunk, ChunkVersion0WithoutSchemaVersionImp.class);
         Dataset<ChunkVersion0WithoutSchemaVersionImp> ds = data.as(Encoders.bean(ChunkVersion0WithoutSchemaVersionImp.class));
//         Dataset<Row> df = ds.toDF();
//
//        Dataset<Row> ds1 = df.groupBy(NAME,CHUNK_DAY).agg(count("*"));
//        ds1.show();







        //JavaRDD<ChunkVersion0> rdd = jsc.parallelize(listChunk);
        //rdd.foreach(System.out::println);

//      List<ChunkVersion0> r = rdd.collect();
//        r.get(0).getId();




    }
}