package com.hurence.timeseries.modele.chunk;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;

import java.nio.charset.StandardCharsets;

public interface Chunk {

    default String getId() {
        String toHash = getValueAsString() +
                getName() +
                getStart();

        return Hashing.sha256()
                .hashString(toHash, StandardCharsets.UTF_8)
                .toString();
    }
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

    /**
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    Chunk truncate(long from, long to);

}
