package com.hurence.historian.modele.stream.impl;

import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonObject;

public class ChunkSolrStreamVersionEVOA0 extends AbstractChunkSolrStream {

    public ChunkSolrStreamVersionEVOA0(JsonSolrStream jsonSolrStream) {
        super(jsonSolrStream);
    }

    @Override
    Chunk convertJsonToChunk(JsonObject json) {
        return new ChunkFromJsonObjectVersionEVOA0(json);
    }
}
