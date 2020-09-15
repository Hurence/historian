package com.hurence.historian.solr.util;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkFromJsonObjectVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ChunkBuilderHelper {


    private ChunkBuilderHelper() {
    }

    public static ChunkVersionCurrent fromPoints(String metricName, List<Point> points) {
        return fromPoints(metricName, new TreeSet<>(points));
    }

    public static ChunkVersionCurrent fromPoints(String metricName, TreeSet<Point> points) {
        PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("test");
        return converter.buildChunk(metricName, points);
    }

    public static ChunkVersionCurrent fromPointsAndTags(String metricName,
                                                        List<Point> points,
                                                        Map<String, String> tags) {
        return fromPointsAndTags(metricName, new TreeSet<>(points), tags);
    }

    public static ChunkVersionCurrent fromPointsAndTags(String metricName,
                                                        TreeSet<Point> points,
                                                        Map<String, String> tags) {
        PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("test");
        return converter.buildChunk(metricName, points, tags);
    }

    public static ChunkVersionCurrent fromJson(String json) {
        JsonObject jsonObject = new JsonObject(json);
        return new ChunkFromJsonObjectVersionCurrent(jsonObject);
    }

}
