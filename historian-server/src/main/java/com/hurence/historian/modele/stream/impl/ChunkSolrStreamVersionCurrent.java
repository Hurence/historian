package com.hurence.historian.modele.stream.impl;

import com.hurence.timeseries.modele.chunk.ChunkFromJsonObjectVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import io.vertx.core.json.JsonObject;

public class ChunkSolrStreamVersionCurrent extends AbstractChunkSolrStream {

    public ChunkSolrStreamVersionCurrent(JsonSolrStream jsonSolrStream) {
        super(jsonSolrStream);
    }

    @Override
    ChunkVersionCurrent convertJsonToChunk(JsonObject json) {
        return new ChunkFromJsonObjectVersionCurrent(json);
    }
}
