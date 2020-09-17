package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;
import com.hurence.timeseries.modele.points.PointImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class is not thread safe !
 */
public class JsonObjectToChunkCurrentVersion {

    private PointsToChunkVersionCurrent converter;

    public JsonObjectToChunkCurrentVersion(String chunkOrigin) {
        this.converter = new PointsToChunkVersionCurrent(chunkOrigin);
    }


    public SolrInputDocument chunkIntoSolrDocument(JsonObject json) {
        TreeSet<Point> points = getPoints(json);
        String name = getName(json);
        Map<String, String> tags = getTags(json);
        ChunkVersionCurrent chunk = converter.buildChunk(name, points, tags);
        return SolrDocumentBuilder.fromChunk(chunk);
    }

    private Map<String, String> getTags(JsonObject json) {
        JsonObject tagsJson = json.getJsonObject(HistorianServiceFields.TAGS, new JsonObject());
        Map<String, String> tags = new HashMap<>();
        tagsJson.getMap().forEach((key, value) -> {
            tags.put(key, value.toString());
        });
        return tags;
    }

    private String getName(JsonObject json) {
        return json.getString(HistorianServiceFields.NAME);
    }

    private TreeSet<Point> getPoints(JsonObject json) {
        JsonArray pointsJson = json.getJsonArray(HistorianServiceFields.POINTS);
        TreeSet<Point> points = new TreeSet<>();
        for (Object point : pointsJson) {
            JsonArray jsonPoint = (JsonArray) point;
            long timestamps = jsonPoint.getLong(0);
            double value = jsonPoint.getDouble(1);
            points.add(new PointImpl(timestamps, value));
        }
        return points;
    }
}