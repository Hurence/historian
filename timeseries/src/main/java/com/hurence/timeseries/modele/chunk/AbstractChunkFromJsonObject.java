package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.solr.Schema;
import com.hurence.historian.modele.solr.SolrField;
import io.vertx.core.json.JsonObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

abstract class AbstractChunkFromJsonObject implements Chunk {

    protected JsonObject chunk;
    private Map<String, String> tags;
    private boolean tagsInitialized = false;

    public AbstractChunkFromJsonObject(JsonObject chunk) {
        this.chunk = chunk;
    }

    @Override
    public boolean containsTag(String tagName) {
        return chunk.containsKey(tagName);
    }

    @Override
    public String getTag(String tagName) {
        return chunk.getString(tagName);
    }

    @Override
    public Map<String, String> getTags() {
        if (tagsInitialized)
            return tags;
        Collection<String> schemaFields = Schema.getChunkSchema(getVersion()).getFields()
                .stream().map(SolrField::getName)
                .collect(Collectors.toList());
        tags = new HashMap<>();
        chunk.fieldNames().forEach(n -> {
            if (schemaFields.contains(n))
                return;
            tags.put(n, chunk.getString(n));
        });
        tagsInitialized = true;
        return tags;
    }

    @Override
    public String toString() {
        return "ChunkFromJsonObjectVersionEVOA0{" +
                "chunk=" + chunk.encodePrettily() +
                '}';
    }

    @Override
    public Chunk truncate(long from, long to) {
        throw new UnsupportedOperationException("This chunk implementation is for reading only");
    }
}
