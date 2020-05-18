package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.historian.compatibility.JsonStreamSolrStreamSchemaVersion0;
import com.hurence.historian.modele.SchemaVersion;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;

public class JsonStreamSolrStream implements JsonStream {

    public static JsonStreamSolrStream forVersion(SchemaVersion version, TupleStream stream) {
        switch (version) {
            case EVOA0:
                return new JsonStreamSolrStreamSchemaVersion0(stream);
            case VERSION_0:
                return new JsonStreamSolrStream(stream);
            default:
                throw new IllegalArgumentException(String.format(
                        "schema version %s for chunks is not yet supported or no longer supported",
                        version));
        }
    }

    private TupleStream stream;
    private long counter = 0L;

    public JsonStreamSolrStream(TupleStream stream) {
        this.stream = stream;
    }

    @Override
    public void open() throws IOException {
        stream.open();
    }

    @Override
    public JsonObject read() throws IOException {
        Tuple tuple = stream.read();
        counter++;
        return toJson(tuple);
    }

    @Override
    public long getNumberOfDocRead() {
        return counter;
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
