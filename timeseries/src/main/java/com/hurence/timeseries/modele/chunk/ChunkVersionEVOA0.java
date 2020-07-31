package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.SchemaVersion;

public interface ChunkVersionEVOA0 extends Chunk {

    default SchemaVersion getVersion() {
        return SchemaVersion.EVOA0;
    }

    String sax();
    boolean trend();
}
