package com.hurence.historian.modele.stream.impl;

import com.hurence.historian.modele.stream.JsonStream;
import com.hurence.historian.modele.SchemaVersion;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;

public class JsonSolrStream implements JsonStream {

    public static JsonSolrStream forVersion(SchemaVersion version, TupleStream stream) {
        switch (version) {
            case VERSION_1:
                return new JsonSolrStream(new SolrStream(stream));
            default:
                throw new IllegalArgumentException(String.format(
                        "schema version %s for chunks is not yet supported or no longer supported",
                        version));
        }
    }

    private SolrStream stream;

    public JsonSolrStream(SolrStream stream) {
        this.stream = stream;
    }

    @Override
    public void open() throws IOException {
        stream.open();
    }

    @Override
    public JsonObject read() throws IOException {
        Tuple tuple = stream.read();
        return toJson(tuple);
    }

    @Override
    public long getCurrentNumberRead() {
        return stream.getCurrentNumberRead();
    }

    @Override
    public boolean hasNext() {
        return stream.hasNext();
    }

    protected JsonObject toJson(Tuple tuple) {
        @SuppressWarnings("unchecked")
        final JsonObject json = new JsonObject(tuple.fields);
        return json;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
