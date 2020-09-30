package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonObject;

public interface TimeSeriesExtracter {

    String TIMESERIE_POINT = HistorianServiceFields.DATAPOINTS;
    String TIMESERIE_AGGS = HistorianServiceFields.AGGREGATION;
    String TOTAL_POINTS = HistorianServiceFields.TOTAL_POINTS;

    void addChunk(Chunk chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonObject getTimeSeries();

    long chunkCount();

    long pointCount();

}
