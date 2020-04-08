package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.historian.compatibility.SchemaVersion;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface JsonStream extends Closeable, Serializable {

    void open() throws IOException;

    JsonObject read() throws IOException;

    long getNumberOfDocRead();
}
