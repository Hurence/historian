package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class ChunkFromJsonObjectVersion0 extends AbstractChunkFromJsonObject implements ChunkVersion0 {


    public ChunkFromJsonObjectVersion0(JsonObject chunk) {
        super(chunk);
    }

    @Override
    public String getName() {
        return chunk.getString(HistorianChunkCollectionFieldsVersion0.NAME);
    }

    @Override
    public String getValueAsString() {
        return chunk.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE);
    }

    @Override
    public byte[] getValueAsBinary() {
        return chunk.getBinary(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE);
    }
    @Override
    public long getStart() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_START);
    }

    @Override
    public long getEnd() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_END);
    }

    @Override
    public long getCount() {
        return chunk.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT);
    }

    @Override
    public double getFirst() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST);
    }

    @Override
    public double getLast() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_LAST);
    }

    @Override
    public double getMin() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN);
    }

    @Override
    public double getMax() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX);
    }

    @Override
    public double getSum() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM);
    }

    @Override
    public double getAvg() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG);
    }

    @Override
    public int getYear() {
        return chunk.getInteger(HistorianChunkCollectionFieldsVersion0.CHUNK_YEAR);
    }

    @Override
    public int getMonth() {
        return chunk.getInteger(HistorianChunkCollectionFieldsVersion0.CHUNK_MONTH);
    }

    @Override
    public String getDay() {
        return chunk.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_DAY);
    }

    @Override
    public double getStddev() {
        return chunk.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_STDDEV);
    }

    @Override
    public String getId() {
        return chunk.getString(HistorianChunkCollectionFieldsVersion0.ID);
    }

    @Override
    public List<String> getCompactionsRunning() {
        return chunk.getJsonArray(HistorianChunkCollectionFieldsVersion0.COMPACTIONS_RUNNING).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public boolean getTrend() {
        return chunk.getBoolean(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND);
    }

    @Override
    public boolean getOutlier() {
        return chunk.getBoolean(HistorianChunkCollectionFieldsVersion0.CHUNK_OUTLIER);
    }

    @Override
    public String getOrigin() {
        return chunk.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN);
    }

    @Override
    public String getSax() {
        return chunk.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX);
    }

    @Override
    public String toString() {
        return "ChunkFromJsonObjectVersion0{" +
                "chunk=" + chunk.encodePrettily() +
                '}';
    }
}
