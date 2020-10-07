package com.hurence.historian.model.stream.impl;


import com.hurence.timeseries.converter.ChunkFromJsonObjectVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonObject;

public class ChunkSolrStreamVersionCurrent extends AbstractChunkSolrStream {

    public ChunkSolrStreamVersionCurrent(JsonSolrStream jsonSolrStream) {
        super(jsonSolrStream);
    }

    @Override
    Chunk convertJsonToChunk(JsonObject json) {
        return new ChunkFromJsonObjectVersionCurrent(json);
    }
}
