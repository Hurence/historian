package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersionEVOA0;
import com.hurence.timeseries.converter.ChunkTruncater;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ChunkFromJsonObjectVersionEVOA0 extends AbstractChunkFromJsonObject implements ChunkVersionEVOA0 {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkFromJsonObjectVersionEVOA0.class);

    public ChunkFromJsonObjectVersionEVOA0(JsonObject chunk) {
        super(chunk);
    }

    @Override
    public String getId() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionEVOA0.ID);
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
    public String getSax() {
        return chunk.getString(HistorianChunkCollectionFieldsVersionEVOA0.CHUNK_SAX);
    }

    @Override
    public boolean getTrend() {
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
        return null;
    }

    @Override
    public ChunkVersionEVOA0 truncate(long from, long to) {
        try {
            return ChunkTruncater.truncate(this, from, to);
        } catch (IOException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }
}
