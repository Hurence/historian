package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.SchemaVersion;

import java.util.List;

public interface ChunkVersion0 extends Chunk {

    default SchemaVersion getVersion() {
        return SchemaVersion.VERSION_0;
    }

    String sax();
    double last();
    double stddev();
    List<String> compactions_running();
    boolean trend();
    boolean outlier();
    String origin();
}
