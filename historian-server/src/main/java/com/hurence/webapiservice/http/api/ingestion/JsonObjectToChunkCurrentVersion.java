package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static com.hurence.historian.model.HistorianServiceFields.POINTS;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;

/**
 * This class is not thread safe !
 */
public class JsonObjectToChunkCurrentVersion {

    private MeasuresToChunkVersionCurrent converter;

    public JsonObjectToChunkCurrentVersion(String chunkOrigin) {
        this.converter = new MeasuresToChunkVersionCurrent(chunkOrigin);
    }

    public SolrInputDocument chunkIntoSolrDocument(JsonObject json) {
        TreeSet<Measure> measures = getPoints(json);
        String name = getName(json);
        Map<String, String> tags = getTags(json);
        Chunk chunk = converter.buildChunk(name, measures, tags);
        return SolrDocumentBuilder.fromChunk(chunk);
    }

    private Map<String, String> getTags(JsonObject json) {
        JsonObject tagsJson = json.getJsonObject(FIELD_TAGS, new JsonObject());
        Map<String, String> tags = new HashMap<>();
        tagsJson.getMap().forEach((key, value) -> {
            tags.put(key, value.toString());
        });
        return tags;
    }

    private String getName(JsonObject json) {
        return json.getString(FIELD_NAME);
    }

    private TreeSet<Measure> getPoints(JsonObject json) {
        JsonArray pointsJson = json.getJsonArray(POINTS);
        TreeSet<Measure> measures = new TreeSet<>();
        for (Object point : pointsJson) {
            JsonArray jsonPoint = (JsonArray) point;
            long timestamps = jsonPoint.getLong(0);
            double value = jsonPoint.getDouble(1);
            try{
                Float quality = jsonPoint.getFloat(2);
                measures.add( Measure.fromValueAndQuality(timestamps, value, quality));
            }catch (Exception ex) {
                measures.add( Measure.fromValue(timestamps, value));
            }
        }
        return measures;
    }
}
