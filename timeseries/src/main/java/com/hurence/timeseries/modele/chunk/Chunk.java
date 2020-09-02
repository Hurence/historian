package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.SchemaVersion;

import java.io.Serializable;
import java.util.Map;

public interface Chunk extends Serializable {

    String getId();
    SchemaVersion getVersion();
    String getName();
    String getValueAsString();
    byte[] getValueAsBinary();
    long getStart();
    long getEnd();
    long getCount();
    double getFirst();
    double getMin();
    double getMax();
    double getSum();
    double getAvg();
    int getYear();
    int getMonth();
    String getDay();

    boolean containsTag(String tagName);
    String getTag(String tagName);
    Map<String, String> getTags();

    /**
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    Chunk truncate(long from, long to);
}
