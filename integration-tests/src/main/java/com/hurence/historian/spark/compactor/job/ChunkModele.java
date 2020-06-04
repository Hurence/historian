package com.hurence.historian.spark.compactor.job;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ChunkModele {

//    public static ChunkModele fromPoints(String metricName, List<Point> points) {
//        return fromPoints(metricName, 2000, 12, 13, "logisland", points);
//    }
//
//    public static ChunkModele fromPoints(String metricName,
//                                         int year,
//                                         int month,
//                                         int day,
//                                         String chunk_origin,
//                                         List<Point> points) {
//        ChunkModele chunk = new ChunkModele();
//        chunk.points = points;
//        chunk.compressedPoints = compressPoints(chunk.points);
//        chunk.start = chunk.points.stream().mapToLong(Point::getTimestamp).min().getAsLong();
//        chunk.end = chunk.points.stream().mapToLong(Point::getTimestamp).max().getAsLong();;
//        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
//        chunk.avg = chunk.sum / chunk.points.size();
//        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
//        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
//        chunk.name = metricName;
//        chunk.sax = "edeebcccdf";
//        chunk.firstValue = points.get(0).getValue();
//        chunk.year = year;
//        chunk.month = month;
//        chunk.day = String.valueOf(day);
//        chunk.chunk_origin = chunk_origin;
//        return chunk;
//    }

    JsonObject toJson(String id);

    public SolrInputDocument buildSolrDocument(String id);
}
