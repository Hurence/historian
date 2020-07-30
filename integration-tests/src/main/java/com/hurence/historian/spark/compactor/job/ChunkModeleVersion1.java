package com.hurence.historian.spark.compactor.job;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion1;
import com.hurence.timeseries.compaction.Compression;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesCurrentSerializer;
import com.hurence.timeseries.compaction.protobuf.ProtoBufTimeSeriesSerializer;
import com.hurence.timeseries.modele.Point;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChunkModeleVersion1 implements ChunkModele {
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
    public int year;
    public int month;
    public String day;
    public String chunk_origin;
    public float chunk_quality_avg;
    public float chunk_quality_sum;
    public float chunk_quality_min;
    public float chunk_quality_max;
    public float chunk_quality_first;
    public Map<String, String> tagsAsKeyValue = new HashMap<>();

    public static ChunkModeleVersion1 fromPoints(String metricName, List<Point> points) {
        return fromPoints(metricName, 2000, 12, 13, "logisland", points);
    }

    /**
     *
     * @param key
     * @param value
     * @return himself for fluent api
     */
    public ChunkModeleVersion1 addTag(String key, String value) {
        tagsAsKeyValue.put(key, value);
        return this;
    }

    public static ChunkModeleVersion1 fromPoints(String metricName,
                                                 int year,
                                                 int month,
                                                 int day,
                                                 String chunk_origin,
                                                 List<Point> points) {
        ChunkModeleVersion1 chunk = new ChunkModeleVersion1();
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
        chunk.year = year;
        chunk.month = month;
        chunk.day = String.valueOf(day);
        chunk.chunk_origin = chunk_origin;
        chunk.chunk_quality_sum = (float) chunk.points.stream().mapToDouble(ChunkModeleVersion1::getQuality).sum();
        chunk.chunk_quality_avg = chunk.chunk_quality_sum / chunk.points.size();
        chunk.chunk_quality_min = (float) chunk.points.stream().mapToDouble(ChunkModeleVersion1::getQuality).min().getAsDouble();
        chunk.chunk_quality_max = (float) chunk.points.stream().mapToDouble(ChunkModeleVersion1::getQuality).max().getAsDouble();
        chunk.chunk_quality_first = getQuality(points.get(0));
        return chunk;
    }

    protected static byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufTimeSeriesCurrentSerializer.to(pointsChunk.iterator(),0 , ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    private static float getQuality(Point point) {
        if (point.hasQuality())
            return point.getQuality();
        else
            return 1f;
    }


    public JsonObject toJson(String id) {
        JsonObject json = new JsonObject();
        this.tagsAsKeyValue.forEach(json::put);
        json.put(HistorianChunkCollectionFieldsVersion1.ID, id);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_START, this.start);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_COUNT, this.points.size());
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_END, this.end);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_SAX, this.sax);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_VALUE, this.compressedPoints);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_AVG, this.avg);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_MIN, this.min);
        json.put(HistorianChunkCollectionFieldsVersion1.NAME, this.name);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_TREND, this.trend);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_MAX, this.max);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_SUM, this.sum);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_FIRST, this.firstValue);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_DAY, this.day);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_MONTH, this.month);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_YEAR, this.year);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_ORIGIN, this.chunk_origin);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_AVG, this.chunk_quality_avg);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MIN, this.chunk_quality_min);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MAX, this.chunk_quality_max);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_SUM, this.chunk_quality_sum);
        json.put(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_FIRST, this.chunk_quality_first);
        return json;
    }

    public SolrInputDocument buildSolrDocument(String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        tagsAsKeyValue.forEach(doc::addField);
        doc.addField(HistorianChunkCollectionFieldsVersion1.ID, id);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_START, this.start);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_COUNT, this.points.size());
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_END, this.end);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_SAX, this.sax);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_VALUE, Base64.getEncoder().encodeToString(this.compressedPoints));
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_AVG, this.avg);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_MIN, this.min);
        doc.addField(HistorianChunkCollectionFieldsVersion1.NAME, this.name);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_TREND, this.trend);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_MAX, this.max);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_SUM, this.sum);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_FIRST, this.firstValue);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_DAY, this.day);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_MONTH, this.month);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_YEAR, this.year);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_ORIGIN, this.chunk_origin);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_AVG, this.chunk_quality_avg);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MIN, this.chunk_quality_min);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_MAX, this.chunk_quality_max);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_SUM, this.chunk_quality_sum);
        doc.addField(HistorianChunkCollectionFieldsVersion1.CHUNK_QUALITY_FIRST, this.chunk_quality_first);
        return doc;
    }
}
