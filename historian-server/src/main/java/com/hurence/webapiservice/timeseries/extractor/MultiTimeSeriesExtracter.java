package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.modele.chunk.Chunk;
import io.vertx.core.json.JsonArray;

public interface MultiTimeSeriesExtracter {

    public static String TIMESERIE_TAGS = "tags";
    public static String TIMESERIE_NAME = "name";

    void addChunk(Chunk chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonArray getTimeSeries();

    long chunkCount();

    long pointCount();

}
