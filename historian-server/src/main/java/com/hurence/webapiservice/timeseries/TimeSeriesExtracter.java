package com.hurence.webapiservice.timeseries;

import com.hurence.historian.modele.HistorianFields;
import io.vertx.core.json.JsonObject;

public interface TimeSeriesExtracter {

    String TIMESERIE_NAME = HistorianFields.NAME;
    String TIMESERIE_POINT = HistorianFields.DATAPOINTS;
    String TIMESERIE_AGGS = HistorianFields.AGGREGATION;

    void addChunk(JsonObject chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonObject getTimeSeries();

    long chunkCount();

    long pointCount();

}
