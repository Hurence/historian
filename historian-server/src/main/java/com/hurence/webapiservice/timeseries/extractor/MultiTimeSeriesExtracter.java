package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import io.vertx.core.json.JsonArray;

public interface MultiTimeSeriesExtracter {

    public static String TIMESERIE_TAGS = "tags";
    public static String TIMESERIE_NAME = "name";

    void addChunk(ChunkVersionCurrent chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonArray getTimeSeries();

    long chunkCount();

    long pointCount();

}
