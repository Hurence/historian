package com.hurence.historian.modele.stream.impl;

import com.hurence.historian.mymodele.Chunk;
import com.hurence.historian.mymodele.ChunkFromJsonObjectVersion0;
import io.vertx.core.json.JsonObject;

public class ChunkSolrStreamVersion0 extends AbstractChunkSolrStream {

    public ChunkSolrStreamVersion0(JsonSolrStream jsonSolrStream) {
        super(jsonSolrStream);
    }

    @Override
    Chunk convertJsonToChunk(JsonObject json) {
        return new ChunkFromJsonObjectVersion0(json);
    }
}
