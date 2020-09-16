package com.hurence.historian.modele.stream.impl;

import com.hurence.timeseries.converter.ChunkFromJsonObjectVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonObject;

public class ChunkSolrStreamVersion0 extends AbstractChunkSolrStream {

    public ChunkSolrStreamVersion0(JsonSolrStream jsonSolrStream) {
        super(jsonSolrStream);
    }

    @Override
    Chunk convertJsonToChunk(JsonObject json) {
        return new ChunkFromJsonObjectVersionCurrent(json);
    }
}
