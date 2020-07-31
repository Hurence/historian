package com.hurence.webapiservice.http.api.ingestion;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.chunk.Chunk;
import com.hurence.historian.modele.solr.SolrFieldMapping;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.timeseries.DateInfo;
import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.TimeSeriesUtil;
import com.hurence.timeseries.converter.PointsToChunkVersion0;
import com.hurence.timeseries.functions.*;
import com.hurence.timeseries.modele.list.DoubleList;
import com.hurence.timeseries.modele.list.LongList;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.metric.MetricType;
import com.hurence.timeseries.modele.points.Point;
import com.hurence.timeseries.modele.points.PointImpl;
import com.hurence.timeseries.query.QueryEvaluator;
import com.hurence.timeseries.query.TypeFunctions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.common.SolrInputDocument;

import java.nio.charset.StandardCharsets;
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
        List<Point> points = getPoints(json);
        String name = getName(json);
        Map<String, String> tags = getTags(json);
        Chunk chunk = converter.buildChunk(name, points, tags);
//        chunk.toSolrDoc();TODO
        return null;
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

    private List<Point> getPoints(JsonObject json) {
        JsonArray pointsJson = json.getJsonArray(HistorianServiceFields.POINTS);
        List<Point> points = new ArrayList<>();
        for (Object point : pointsJson) {
            JsonArray jsonPoint = (JsonArray) point;
            long timestamps = jsonPoint.getLong(0);
            double value = jsonPoint.getDouble(1);
            points.add(new PointImpl(timestamps, value));
        }
        return points;
    }

    /**
     * calculate sha256 of value, start and name
     *
     * @param doc
     * @return
     */
    private String calulateHash(SolrInputDocument doc) {
        String toHash = doc.getField(mapping.CHUNK_VALUE_FIELD).toString() +
                doc.getField(mapping.CHUNK_NAME).toString() +
                doc.getField(mapping.CHUNK_START_FIELD).toString() +
                doc.getField(HistorianServiceFields.ORIGIN).toString();

        String sha256hex = Hashing.sha256()
                .hashString(toHash, StandardCharsets.UTF_8)
                .toString();

        return sha256hex;
    }
}
