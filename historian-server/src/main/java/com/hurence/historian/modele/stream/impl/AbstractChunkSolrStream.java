package com.hurence.historian.modele.stream.impl;

import com.hurence.historian.modele.stream.ChunkStream;
import com.hurence.timeseries.model.Chunk;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

public abstract class AbstractChunkSolrStream implements ChunkStream {

    private JsonSolrStream stream;

    protected AbstractChunkSolrStream(JsonSolrStream stream) {
        this.stream = stream;
    }

    @Override
    public void open() throws IOException {
        stream.open();
    }

    @Override
    public Chunk read() throws IOException {
        JsonObject json = stream.read();
        return toChunk(json);
    }

    @Override
    public long getCurrentNumberRead() {
        return stream.getCurrentNumberRead();
    }

    @Override
    public boolean hasNext() {
        return stream.hasNext();
    }

    protected Chunk toChunk(JsonObject json) {
        return convertJsonToChunk(json);
    }

    abstract Chunk convertJsonToChunk(JsonObject json);

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
