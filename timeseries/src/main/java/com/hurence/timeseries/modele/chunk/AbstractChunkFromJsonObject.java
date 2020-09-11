package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.solr.Schema;
import com.hurence.historian.modele.solr.SolrField;
import com.hurence.timeseries.converter.ChunkTruncater;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

abstract class AbstractChunkFromJsonObject implements Chunk {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChunkFromJsonObject.class);

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
}
