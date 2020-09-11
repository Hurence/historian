package com.hurence.timeseries.modele.chunk;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;

import java.nio.charset.StandardCharsets;

public interface ChunkVersionEVOA0 extends Chunk {

    default SchemaVersion getVersion() {
        return SchemaVersion.EVOA0;
    }

    String getSax();
    boolean getTrend();

    @Override
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
