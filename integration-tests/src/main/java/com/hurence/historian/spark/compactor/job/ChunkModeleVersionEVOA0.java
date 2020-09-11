package com.hurence.historian.spark.compactor.job;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.compaction.Compression;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesSerializer;
import com.hurence.timeseries.modele.points.PointImpl;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.TreeSet;

public class ChunkModeleVersionEVOA0 implements ChunkModele {
    private static int ddcThreshold = 0;

    public TreeSet<PointImpl> points;
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
    public int year;
    public int month;
    public String day;
    public String chunk_origin;
    public List<String> tags = new ArrayList<>();

    public static ChunkModeleVersionEVOA0 fromPoints(String metricName, List<PointImpl> points) {
        return fromPoints(metricName, 2000, 12, 13, "logisland", points);
    }

    public static ChunkModeleVersionEVOA0 fromJson(String json) throws IOException {
        return fromJson(new JsonObject(json));
    }

    public static ChunkModeleVersionEVOA0 fromJson(JsonObject json) throws IOException {
        ChunkModeleVersionEVOA0 chunk = new ChunkModeleVersionEVOA0();
        chunk.name = json.getString(HistorianChunkCollectionFieldsVersionEVOA0.NAME);
        chunk.year = json.getInteger(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR, 0);
        chunk.month = json.getInteger(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH, 0);
        chunk.day = json.getString(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY);
        chunk.chunk_origin = json.getString(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_ORIGIN);
        chunk.firstValue = json.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST);
        chunk.sum = json.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM);
        chunk.max = json.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MAX);
        chunk.min = json.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MIN);
        chunk.avg = json.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG);
        chunk.end = json.getLong(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END);
        chunk.start = json.getLong(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START);
        chunk.compressedPoints = json.getBinary(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_VALUE);
        chunk.points = BinaryCompactionUtil.unCompressPoints(chunk.compressedPoints, chunk.start, chunk.end);
        return chunk;
    }

    public ChunkModeleVersionEVOA0 addTag(String tag) {
        tags.add(tag);
        return this;
    }

    public static ChunkModeleVersionEVOA0 fromPoints(String metricName,
                                                 int year,
                                                 int month,
                                                 int day,
                                                 String chunk_origin,
                                                 List<PointImpl> points) {
        return fromPoints(metricName, year, month, day, chunk_origin, new TreeSet<>(points));
    }

    public static ChunkModeleVersionEVOA0 fromPoints(String metricName,
                                                     int year,
                                                     int month,
                                                     int day,
                                                     String chunk_origin,
                                                     TreeSet<PointImpl> points) {
        ChunkModeleVersionEVOA0 chunk = new ChunkModeleVersionEVOA0();
        chunk.points = points;
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = chunk.points.stream().mapToLong(PointImpl::getTimestamp).min().getAsLong();
        chunk.end = chunk.points.stream().mapToLong(PointImpl::getTimestamp).max().getAsLong();;
        chunk.sum = chunk.points.stream().mapToDouble(PointImpl::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(PointImpl::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(PointImpl::getValue).max().getAsDouble();
        chunk.name = metricName;
        chunk.sax = "edeebcccdf";
        chunk.firstValue = points.first().getValue();
        chunk.year = year;
        chunk.month = month;
        chunk.day = String.valueOf(day);
        chunk.chunk_origin = chunk_origin;
        return chunk;
    }

    protected static byte[] compressPoints(TreeSet<PointImpl> pointsChunk) {
        byte[] serializedPoints = ProtoBufTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }


    public JsonObject toJson(String id) {
        JsonObject json = new JsonObject();
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.TAGS, tags);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.ID, id);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START, this.start);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE, this.points.size());
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END, this.end);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SAX, this.sax);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_VALUE, this.compressedPoints);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG, this.avg);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MIN, this.min);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.NAME, this.name);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND, this.trend);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MAX, this.max);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM, this.sum);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST, this.firstValue);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY, this.day);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH, this.month);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR, this.year);
        json.put(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_ORIGIN, this.chunk_origin);
        return json;
    }

    public SolrInputDocument buildSolrDocument(String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.TAGS, tags);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.ID, id);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START, this.start);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE, this.points.size());
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END, this.end);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SAX, this.sax);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_VALUE, Base64.getEncoder().encodeToString(this.compressedPoints));
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG, this.avg);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MIN, this.min);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.NAME, this.name);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND, this.trend);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MAX, this.max);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM, this.sum);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST, this.firstValue);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY, this.day);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH, this.month);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR, this.year);
        doc.addField(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_ORIGIN, this.chunk_origin);
        return doc;
    }
}

