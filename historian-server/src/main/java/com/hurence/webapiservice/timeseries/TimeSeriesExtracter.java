package com.hurence.webapiservice.timeseries;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface TimeSeriesExtracter {

    String TIMESERIE_NAME = HistorianFields.NAME;
    String TIMESERIE_POINT = HistorianFields.DATAPOINTS;
    String TIMESERIE_AGGS = "aggs";

    void addChunk(JsonObject chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonObject getTimeSeries();

    long chunkCount();

    long pointCount();

}
