package com.hurence.timeseries.modele.chunk;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public interface ChunkVersionCurrent extends Chunk {

    default SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    String getId();
    String getName();
    //compressed data
    String getValueAsString();
    byte[] getValueAsBinary();
    //start and end
    long getStart();
    long getEnd();
    //value aggs
    long getCount();
    double getFirst();
    double getMin();
    double getMax();
    double getSum();
    double getAvg();
    String getSax();
    double getLast();
    double getStddev();
    boolean getTrend();
    boolean getOutlier();
    //quality aggs
    float getQualityMin();
    float getQualityMax();
    float getQualitySum();
    float getQualityFirst();
    float getQualityAvg();
    //time
    int getYear();
    int getMonth();
    String getDay();
    //tags
    boolean containsTag(String tagName);
    String getTag(String tagName);
    Map<String, String> getTags();
    //is any compaction running ?
    List<String> getCompactionsRunning();
    //origin of the chunk
    String getOrigin();

    /**
     *
     * @param from the desired chunk_start of new chunk
     * @param to the desired chunk_end of new chunk
     * @return a new Chunk without point outside of boundaries
     * (recompute aggregations as well)
     */
    ChunkVersionCurrent truncate(long from, long to);

    static String buildId(ChunkVersionCurrent chunk) {
        String toHash = chunk.getValueAsString() +
                chunk.getName() +
                chunk.getStart() +
                chunk.getOrigin();

        return Hashing.sha256()
                .hashString(toHash, StandardCharsets.UTF_8)
                .toString();
    }
}
