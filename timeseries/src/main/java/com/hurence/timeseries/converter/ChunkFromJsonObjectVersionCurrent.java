package com.hurence.timeseries.converter;


import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.modele.solr.Schema;
import com.hurence.historian.modele.solr.SolrField;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.HistorianChunkCollectionFieldsVersionCurrent;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChunkFromJsonObjectVersionCurrent  {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkFromJsonObjectVersionCurrent.class);

    protected JsonObject chunk;
    private Map<String, String> tags;
    private boolean tagsInitialized = false;


    public boolean containsTag(String tagName) {
        return chunk.containsKey(tagName);
    }

    public String getTag(String tagName) {
        return chunk.getString(tagName);
    }

    public Map<String, String> getTags() {
        if (tagsInitialized)
            return tags;
        Collection<String> schemaFields = Schema.getChunkSchema(getVersion()).getFields()
                .stream().map(SolrField::getName)
                .collect(Collectors.toList());
        tags = new HashMap<>();
        chunk.fieldNames().forEach(n -> {
            if (schemaFields.contains(n))
                return;
            tags.put(n, chunk.getString(n));
        });
        tagsInitialized = true;
        return tags;
    }

    public ChunkFromJsonObjectVersionCurrent(JsonObject chunk) {
        this.chunk = chunk;
    }

    public String getName() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.NAME);
    }

    public String getValueAsString() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_VALUE);
    }

    public byte[] getValueAsBinary() {
        return chunk.getBinary(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_VALUE);
    }

    public long getStart() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_START);
    }

    public long getEnd() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_END);
    }

    public long getCount() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_COUNT);
    }

    public double getFirst() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_FIRST);
    }

    public double getLast() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_LAST);
    }

    public double getMin() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MIN);
    }

    public double getMax() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MAX);
    }

    public double getSum() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SUM);
    }

    public double getAvg() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_AVG);
    }

    public int getYear() {
        return chunk.getInteger(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_YEAR);
    }

    public int getMonth() {
        return chunk.getInteger(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MONTH);
    }

    public String getDay() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_DAY);
    }

    public double getStddev() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_STDDEV);
    }

    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    public String getId() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.ID);
    }

    public List<String> getCompactionsRunning() {
        return chunk.getJsonArray(HistorianChunkCollectionFieldsVersionCurrent.COMPACTIONS_RUNNING).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    public boolean getTrend() {
        return chunk.getBoolean(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_TREND);
    }

    public boolean getOutlier() {
        return chunk.getBoolean(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_OUTLIER);
    }

    public float getQualityMin() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MIN);
    }

    public float getQualityMax() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MAX);
    }

    public float getQualitySum() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_SUM);
    }

    public float getQualityFirst() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_FIRST);
    }

    public float getQualityAvg() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_AVG);
    }

    public String getOrigin() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_ORIGIN);
    }

    public String getSax() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SAX);
    }

    @Override
    public String toString() {
        return "ChunkFromJsonObjectVersionCurrent{" +
                "chunk=" + chunk.encodePrettily() +
                '}';
    }


}