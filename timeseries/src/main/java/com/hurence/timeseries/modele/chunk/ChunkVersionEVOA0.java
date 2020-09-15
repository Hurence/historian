package com.hurence.timeseries.modele.chunk;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public interface ChunkVersionEVOA0 extends Chunk {

    default SchemaVersion getVersion() {
        return SchemaVersion.EVOA0;
    }

    String getId();
    String getName();
//    comprresed points
    String getValueAsString();
    byte[] getValueAsBinary();
    //start end
    long getStart();
    long getEnd();
    //value aggs
    String getSax();
    boolean getTrend();
    long getCount();
    double getFirst();
    double getMin();
    double getMax();
    double getSum();
    double getAvg();
    //date info
    int getYear();
    int getMonth();
    String getDay();
    //tags
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
    ChunkVersionEVOA0 truncate(long from, long to);

    static String buildId(ChunkVersionEVOA0 chunk) {
        String toHash = chunk.getValueAsString() +
                chunk.getName() +
                chunk.getStart();

        return Hashing.sha256()
                .hashString(toHash, StandardCharsets.UTF_8)
                .toString();
    }
}
