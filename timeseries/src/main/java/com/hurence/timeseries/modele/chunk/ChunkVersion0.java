package com.hurence.timeseries.modele.chunk;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;

import java.nio.charset.StandardCharsets;
import java.util.List;

public interface ChunkVersion0 extends Chunk {

    @Override
    default SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    String getSax();
    double getLast();
    double getStddev();
    List<String> getCompactionsRunning();
    boolean getTrend();
    boolean getOutlier();
    String getOrigin();

    @Override
    ChunkVersion0 truncate(long from, long to);

    static String buildId(ChunkVersion0 chunk) {
        String toHash = chunk.getValueAsString() +
                chunk.getName() +
                chunk.getStart() +
                chunk.getOrigin();

        return Hashing.sha256()
                .hashString(toHash, StandardCharsets.UTF_8)
                .toString();
    }
}
