package com.hurence.webapiservice.timeseries.extractor;

import io.vertx.core.json.JsonObject;

public interface TimeSeriesExtracter {

    String TIMESERIE_POINT = "datapoints";
    String TIMESERIE_AGGS = "aggregation";

    void addChunk(JsonObject chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonObject getTimeSeries();

    long chunkCount();

    long pointCount();

}
