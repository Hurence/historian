package com.hurence.historian.spark.compactor.job;

import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

public interface ChunkModele {

//    public static ChunkModele fromPoints(String metricName, List<Measure> measures) {
//        return fromPoints(metricName, 2000, 12, 13, "logisland", measures);
//    }
//
//    public static ChunkModele fromPoints(String metricName,
//                                         int year,
//                                         int month,
//                                         int day,
//                                         String chunk_origin,
//                                         List<Measure> measures) {
//        ChunkModele chunk = new ChunkModele();
//        chunk.measures = measures;
//        chunk.compressedPoints = compressPoints(chunk.measures);
//        chunk.start = chunk.measures.stream().mapToLong(Measure::getTimestamp).min().getAsLong();
//        chunk.end = chunk.measures.stream().mapToLong(Measure::getTimestamp).max().getAsLong();;
//        chunk.sum = chunk.measures.stream().mapToDouble(Measure::getValue).sum();
//        chunk.avg = chunk.sum / chunk.measures.size();
//        chunk.min = chunk.measures.stream().mapToDouble(Measure::getValue).min().getAsDouble();
//        chunk.max = chunk.measures.stream().mapToDouble(Measure::getValue).max().getAsDouble();
//        chunk.name = metricName;
//        chunk.sax = "edeebcccdf";
//        chunk.firstValue = measures.get(0).getValue();
//        chunk.year = year;
//        chunk.month = month;
//        chunk.day = String.valueOf(day);
//        chunk.chunk_origin = chunk_origin;
//        return chunk;
//    }

    JsonObject toJson(String id);

    public SolrInputDocument buildSolrDocument(String id);
}
