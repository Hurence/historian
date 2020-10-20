package com.hurence.historian.solr.util;

import com.hurence.timeseries.converter.ChunkFromJsonObjectVersionCurrent;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ChunkBuilderHelper {


    private ChunkBuilderHelper() {
    }

    public static List<Chunk> fromGroupOfMeasures(String metric_name, List<List<Measure>> pointsByChunk) {
        return pointsByChunk.stream()
                .map(points -> {
                    return ChunkBuilderHelper.fromPoints(metric_name, points);
                }).collect(Collectors.toList());
    }

    public static Chunk fromPoints(String metricName, List<Measure> points) {
        return fromPoints(metricName, new TreeSet<>(points));
    }

    public static Chunk fromPoints(String metricName, List<Measure> points, String origin) {
        return fromPoints(metricName, new TreeSet<>(points), origin);
    }

    public static Chunk fromPoints(String metricName, TreeSet<Measure> points) {
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent("test");
        return fromPoints(metricName, points, "test");
    }

    public static Chunk fromPoints(String metricName, TreeSet<Measure> points, String origin) {
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent(origin);
        return converter.buildChunk(metricName, points);
    }

    public static Chunk fromPointsAndTags(String metricName,
                                                        List<Measure> points,
                                                        Map<String, String> tags) {
        return fromPointsAndTags(metricName, new TreeSet<>(points), tags);
    }

    public static Chunk fromPointsAndTags(String metricName,
                                          List<Measure> points,
                                          Map<String, String> tags,
                                          String origin) {
        return fromPointsAndTags(metricName, new TreeSet<>(points), tags, origin);
    }

    public static Chunk fromPointsAndTags(String metricName,
                                                        TreeSet<Measure> points,
                                                        Map<String, String> tags) {
        return fromPointsAndTags(metricName, points, tags, "test");
    }

    public static Chunk fromPointsAndTags(String metricName,
                                          TreeSet<Measure> points,
                                          Map<String, String> tags,
                                          String origin) {
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent(origin);
        return converter.buildChunk(metricName, points, tags);
    }

    public static Chunk fromJson(String json) {
        JsonObject jsonObject = new JsonObject(json);
        return new ChunkFromJsonObjectVersionCurrent(jsonObject);
    }

    public static Chunk fromCompressedPoints(String metricName, byte[] compressedPoints, long chunkStart, long chunkEnd) throws IOException {
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent("test");
        return converter.buildChunkFromCompressedPoints(metricName, compressedPoints, chunkStart, chunkEnd);
    }

}
