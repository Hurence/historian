package com.hurence.historian.model.stream.impl;

import com.hurence.historian.model.stream.Stream;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;

public class SolrStream implements Stream<Tuple> {

    private TupleStream stream;
    private long counter = 0L;
    private Tuple currentTuple;

    public SolrStream(TupleStream stream) {
        this.stream = stream;
    }

    @Override
    public void open() throws IOException {
        stream.open();
    }

    @Override
    public Tuple read() throws IOException {
        this.currentTuple = stream.read();
        counter++;
        return currentTuple;
    }

    @Override
    public long getCurrentNumberRead() {
        return counter;
    }

    @Override
    public boolean hasNext() {
        return !currentTuple.EOF;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
