package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ChunkFromJsonObjectVersionEVOA0 implements ChunkVersionEVOA0 {

    JsonObject chunk;

    public ChunkFromJsonObjectVersionEVOA0(JsonObject chunk) {
        this.chunk = chunk;
    }

    @Override
    public String getName() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionEVOA0.NAME);
    }

    @Override
    public String getValueAsString() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_VALUE);
    }

    @Override
    public byte[] getValueAsBinary() {
        return chunk.getBinary(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_VALUE);
    }

    @Override
    public long getStart() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_START);
    }

    @Override
    public long getEnd() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_END);
    }

    @Override
    public long getCount() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SIZE,
                chunk.getLong(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_COUNT));
    }

    @Override
    public double getFirst() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_FIRST);
        return (double) obj;
    }

    @Override
    public double getMin() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MIN);
    }

    @Override
    public double getMax() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MAX);
    }

    @Override
    public double getSum() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SUM);
        return (double) obj;
    }

    @Override
    public double getAvg() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_AVG);
        return (double) obj;
    }

    @Override
    public int getYear() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_YEAR);
        return (int) obj;
    }

    @Override
    public int getMonth() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_MONTH);
        return (int) obj;
    }

    @Override
    public String getDay() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_DAY);
        return (String) obj;
    }

    @Override
    public boolean containsTag(String tagName) {
        return chunk.containsKey(tagName);
    }

    @Override
    public String getTag(String tagName) {
        return chunk.getString(tagName);
    }

    @Override
    public String sax() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SAX);
    }

    @Override
    public boolean trend() {
        Object obj = getObjectOrFirstObjectIfItIsAnArray(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_TREND);
        return (boolean) obj;
    }

    @Override
    public String toString() {
        return "ChunkFromJsonObjectVersionEVOA0{" +
                "chunk=" + chunk.encodePrettily() +
                '}';
    }

    private Object getObjectOrFirstObjectIfItIsAnArray(String fieldName) {
        if (chunk.containsKey(fieldName)) {
            Object value = chunk.getValue(fieldName);
            if (value instanceof JsonArray) {
                JsonArray array = chunk.getJsonArray(fieldName);
                if (array != null && !array.isEmpty()) {
                    return array.getValue(0);
                }
            } else {
                return value;
            }
        }
    }
}
