package com.hurence.historian.modele;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.Base64;
import java.util.List;

public class ChunkModele {
    private static int ddcThreshold = 0;

    public List<Point> points;
    public byte[] compressedPoints;
    public long start;
    public long end;
    public double avg;
    public double min;
    public double max;
    public double sum;
    public double firstValue;
    public boolean trend;
    public String name;
    public String sax;
    public List<String> tags;

public static ChunkModele fromPoints(String metricName, List<Point> points) {
    ChunkModele chunk = new ChunkModele();
    chunk.points = points;
    chunk.compressedPoints = compressPoints(chunk.points);
    chunk.start = chunk.points.stream().mapToLong(Point::getTimestamp).min().getAsLong();
    chunk.end = chunk.points.stream().mapToLong(Point::getTimestamp).max().getAsLong();;
    chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
    chunk.avg = chunk.sum / chunk.points.size();
    chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
    chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
    chunk.name = metricName;
    chunk.sax = "edeebcccdf";
    chunk.firstValue = points.get(0).getValue();
    return chunk;
}

    protected static byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }


    public JsonObject toJson(String id) {
        JsonObject json = new JsonObject();
        json.put(HistorianFields.RESPONSE_CHUNK_ID_FIELD, id);
        json.put(HistorianFields.RESPONSE_CHUNK_START_FIELD, this.start);
        json.put(HistorianFields.RESPONSE_CHUNK_SIZE_FIELD, this.points.size());
        json.put(HistorianFields.RESPONSE_CHUNK_END_FIELD, this.end);
        json.put(HistorianFields.RESPONSE_CHUNK_SAX_FIELD, this.sax);
        json.put(HistorianFields.RESPONSE_CHUNK_VALUE_FIELD, this.compressedPoints);
        json.put(HistorianFields.RESPONSE_CHUNK_AVG_FIELD, this.avg);
        json.put(HistorianFields.RESPONSE_CHUNK_MIN_FIELD, this.min);
        json.put(HistorianFields.RESPONSE_CHUNK_WINDOW_MS_FIELD, 11855);
        json.put(HistorianFields.RESPONSE_METRIC_NAME_FIELD, this.name);
        json.put(HistorianFields.RESPONSE_CHUNK_TREND_FIELD, this.trend);
        json.put(HistorianFields.RESPONSE_CHUNK_MAX_FIELD, this.max);
        json.put(HistorianFields.RESPONSE_CHUNK_SIZE_BYTES_FIELD, this.compressedPoints.length);
        json.put(HistorianFields.RESPONSE_CHUNK_SUM_FIELD, this.sum);
        json.put(HistorianFields.RESPONSE_TAG_NAME_FIELD, this.tags);
        json.put(HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD, this.firstValue);
        return json;
    }

    public SolrInputDocument buildSolrDocument(String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(HistorianFields.RESPONSE_CHUNK_ID_FIELD, id);
        doc.addField(HistorianFields.RESPONSE_CHUNK_START_FIELD, this.start);
        doc.addField(HistorianFields.RESPONSE_CHUNK_SIZE_FIELD, this.points.size());
        doc.addField(HistorianFields.RESPONSE_CHUNK_END_FIELD, this.end);
        doc.addField(HistorianFields.RESPONSE_CHUNK_SAX_FIELD, this.sax);
//        doc.addField(RESPONSE_CHUNK_VALUE_FIELD, this.compressedPoints);
        doc.addField(HistorianFields.RESPONSE_CHUNK_VALUE_FIELD, Base64.getEncoder().encodeToString(this.compressedPoints));
        doc.addField(HistorianFields.RESPONSE_CHUNK_AVG_FIELD, this.avg);
        doc.addField(HistorianFields.RESPONSE_CHUNK_MIN_FIELD, this.min);
        doc.addField(HistorianFields.RESPONSE_CHUNK_WINDOW_MS_FIELD, 11855);
        doc.addField(HistorianFields.RESPONSE_METRIC_NAME_FIELD, this.name);
        doc.addField(HistorianFields.RESPONSE_CHUNK_TREND_FIELD, this.trend);
        doc.addField(HistorianFields.RESPONSE_CHUNK_MAX_FIELD, this.max);
        doc.addField(HistorianFields.RESPONSE_CHUNK_SIZE_BYTES_FIELD, this.compressedPoints.length);
        doc.addField(HistorianFields.RESPONSE_CHUNK_SUM_FIELD, this.sum);
        doc.addField(HistorianFields.RESPONSE_TAG_NAME_FIELD, this.tags);
        doc.addField(HistorianFields.RESPONSE_CHUNK_FIRST_VALUE_FIELD, this.firstValue);
        return doc;
    }
}
