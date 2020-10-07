package com.hurence.timeseries.converter;


import com.hurence.historian.model.HistorianChunkCollectionFieldsVersionCurrent;
import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.model.solr.Schema;
import com.hurence.historian.model.solr.SolrField;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ChunkFromJsonObjectVersionCurrent extends Chunk {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkFromJsonObjectVersionCurrent.class);

    protected JsonObject chunk;
    private Map<String, String> tags;
    private boolean tagsInitialized = false;


    @Override
    public boolean containsTag(String tagName) {
        return chunk.containsKey(tagName);
    }

    @Override
    public String getTag(String tagName) {
        return chunk.getString(tagName);
    }

    @Override
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

    @Override
    public String getName() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.NAME);
    }

    @Override
    public String getValueAsString() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_VALUE);
    }

    @Override
    public byte[] getValue() {
        return chunk.getBinary(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_VALUE);
    }

    @Override
    public long getStart() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_START);
    }

    @Override
    public long getEnd() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_END);
    }

    @Override
    public long getCount() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_COUNT);
    }

    @Override
    public double getFirst() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_FIRST);
    }

    @Override
    public double getLast() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_LAST);
    }

    @Override
    public double getMin() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MIN);
    }

    @Override
    public double getMax() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MAX);
    }

    @Override
    public double getSum() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_SUM);
    }

    @Override
    public double getAvg() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_AVG);
    }

    @Override
    public int getYear() {
        return chunk.getInteger(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_YEAR);
    }

    @Override
    public int getMonth() {
        return chunk.getInteger(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_MONTH);
    }

    @Override
    public String getDay() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_DAY);
    }

    @Override
    public double getStdDev() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_STDDEV);
    }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.getCurrentVersion();
    }

    @Override
    public String getId() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.ID);
    }

   /* public List<String> getCompactionsRunnings() {
        return chunk.getJsonArray(HistorianChunkCollectionFieldsVersionCurrent.COMPACTIONS_RUNNING).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }*/

    @Override
    public boolean isTrend() {
        return chunk.getBoolean(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_TREND);
    }

    @Override
    public boolean isOutlier() {
        return chunk.getBoolean(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_OUTLIER);
    }

    @Override
    public float getQualityMin() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MIN);
    }

    @Override
    public float getQualityMax() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_MAX);
    }

    @Override
    public float getQualitySum() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_SUM);
    }

    @Override
    public float getQualityFirst() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_FIRST);
    }

    @Override
    public float getQualityAvg() {
        return chunk.getFloat(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_QUALITY_AVG);
    }

    @Override
    public String getOrigin() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionCurrent.CHUNK_ORIGIN);
    }

    @Override
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