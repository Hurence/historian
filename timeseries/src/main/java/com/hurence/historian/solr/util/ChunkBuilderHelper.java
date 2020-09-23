package com.hurence.historian.solr.util;

import com.hurence.timeseries.converter.ChunkFromJsonObjectVersionCurrent;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ChunkBuilderHelper {


    private ChunkBuilderHelper() {
    }

    public static Chunk fromPoints(String metricName, List<Measure> points) {
        return fromPoints(metricName, new TreeSet<>(points));
    }

    public static Chunk fromPoints(String metricName, TreeSet<Measure> points) {
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent("test");
        return converter.buildChunk(metricName, points);
    }

    public static Chunk fromPointsAndTags(String metricName,
                                                        List<Measure> points,
                                                        Map<String, String> tags) {
        return fromPointsAndTags(metricName, new TreeSet<>(points), tags);
    }

    public static Chunk fromPointsAndTags(String metricName,
                                                        TreeSet<Measure> points,
                                                        Map<String, String> tags) {
        MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent("test");
        return converter.buildChunk(metricName, points, tags);
    }

    public static Chunk fromJson(String json) {
        JsonObject jsonObject = new JsonObject(json);
        return new ChunkFromJsonObjectVersionCurrent(jsonObject);
    }

}
