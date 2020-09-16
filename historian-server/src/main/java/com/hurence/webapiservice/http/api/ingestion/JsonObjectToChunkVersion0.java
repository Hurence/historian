package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.timeseries.converter.PointsToChunkVersion0;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.*;

/**
 * This class is not thread safe !
 */
public class JsonObjectToChunkVersion0 {

    private PointsToChunkVersion0 converter;

    public JsonObjectToChunkVersion0(String chunkOrigin) {
        this.converter = new PointsToChunkVersion0(chunkOrigin);
    }


    public SolrInputDocument chunkIntoSolrDocument(JsonObject json) {
        TreeSet<Measure> measures = getPoints(json);
        String name = getName(json);
        Map<String, String> tags = getTags(json);
        Chunk chunk = converter.buildChunk(name, measures, tags);
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

    private TreeSet<Measure> getPoints(JsonObject json) {
        JsonArray pointsJson = json.getJsonArray(HistorianServiceFields.POINTS);
        TreeSet<Measure> measures = new TreeSet<>();
        for (Object point : pointsJson) {
            JsonArray jsonPoint = (JsonArray) point;
            long timestamps = jsonPoint.getLong(0);
            double value = jsonPoint.getDouble(1);
            measures.add(new Measure(timestamps, value));
        }
        return measures;
    }
}
