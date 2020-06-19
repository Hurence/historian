package com.hurence.webapiservice.timeseries.extractor;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface MultiTimeSeriesExtracter {

    public static String TIMESERIE_TAGS = "tags";
    public static String TIMESERIE_NAME = "name";

    void addChunk(JsonObject chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonArray getTimeSeries();

    long chunkCount();

    long pointCount();

}
