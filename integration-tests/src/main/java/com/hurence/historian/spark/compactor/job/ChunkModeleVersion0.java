package com.hurence.historian.spark.compactor.job;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.timeseries.modele.chunk.Chunk;
import com.hurence.timeseries.modele.chunk.ChunkFromJsonObjectVersion0;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.compaction.Compression;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesSerializer;
import com.hurence.timeseries.modele.points.PointImpl;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.*;

public class ChunkModeleVersion0 implements ChunkModele {
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
    public Map<String, String> tagsAsKeyValue = new HashMap<>();

    public static ChunkModeleVersion0 fromPoints(String metricName, List<PointImpl> points) {
        return fromPoints(metricName, 2000, 12, 13, "logisland", points);
    }

    public static ChunkModeleVersion0 fromJson(String json) throws IOException {
        JsonObject jsonObject = new JsonObject(json);
        ChunkModeleVersion0 chunk = new ChunkModeleVersion0();
        chunk.name = jsonObject.getString(HistorianChunkCollectionFieldsVersion0.NAME);
        chunk.year = jsonObject.getInteger(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR, 0);
        chunk.month = jsonObject.getInteger(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH, 0);
        chunk.day = jsonObject.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY);
        chunk.chunk_origin = jsonObject.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN);
        chunk.firstValue = jsonObject.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST);
        chunk.sum = jsonObject.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM);
        chunk.max = jsonObject.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX);
        chunk.min = jsonObject.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN);
        chunk.avg = jsonObject.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG);
        chunk.end = jsonObject.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_END);
        chunk.start = jsonObject.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_START);
        chunk.compressedPoints = jsonObject.getBinary(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE);
        chunk.points = BinaryCompactionUtil.unCompressPoints(chunk.compressedPoints, chunk.start, chunk.end);
        return chunk;
    }

    /**
     *
     * @param key
     * @param value
     * @return himself for fluent api
     */
    public ChunkModeleVersion0 addTag(String key, String value) {
        tagsAsKeyValue.put(key, value);
        return this;
    }

    public static ChunkModeleVersion0 fromPoints(String metricName,
                                                 int year,
                                                 int month,
                                                 int day,
                                                 String chunk_origin,
                                                 List<PointImpl> points) {
        ChunkModeleVersion0 chunk = new ChunkModeleVersion0();
        chunk.points = new TreeSet<>(points);
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = chunk.points.stream().mapToLong(PointImpl::getTimestamp).min().getAsLong();
        chunk.end = chunk.points.stream().mapToLong(PointImpl::getTimestamp).max().getAsLong();;
        chunk.sum = chunk.points.stream().mapToDouble(PointImpl::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(PointImpl::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(PointImpl::getValue).max().getAsDouble();
        chunk.name = metricName;
        chunk.sax = "edeebcccdf";
        chunk.firstValue = points.get(0).getValue();
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
        this.tagsAsKeyValue.forEach(json::put);
        json.put(HistorianChunkCollectionFieldsVersion0.ID, id);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_START, this.start);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT, this.points.size());
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_END, this.end);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX, this.sax);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE, this.compressedPoints);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG, this.avg);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN, this.min);
        json.put(HistorianChunkCollectionFieldsVersion0.NAME, this.name);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND, this.trend);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX, this.max);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM, this.sum);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST, this.firstValue);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY, this.day);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH, this.month);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR, this.year);
        json.put(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN, this.chunk_origin);
        return json;
    }

    public Chunk toChunk(String id) {
        return new ChunkFromJsonObjectVersion0(toJson(id));
    }


    public SolrInputDocument buildSolrDocument(String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        tagsAsKeyValue.forEach(doc::addField);
        doc.addField(HistorianChunkCollectionFieldsVersion0.ID, id);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_START, this.start);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT, this.points.size());
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_END, this.end);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX, this.sax);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE, Base64.getEncoder().encodeToString(this.compressedPoints));
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG, this.avg);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN, this.min);
        doc.addField(HistorianChunkCollectionFieldsVersion0.NAME, this.name);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND, this.trend);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX, this.max);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM, this.sum);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST, this.firstValue);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY, this.day);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH, this.month);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR, this.year);
        doc.addField(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN, this.chunk_origin);
        return doc;
    }
}

