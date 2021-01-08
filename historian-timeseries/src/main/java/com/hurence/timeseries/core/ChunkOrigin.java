package com.hurence.timeseries.core;

/**
 * Represents the origin of the chunk. The origin is the entity
 * that created and inserted the chunk in the DB.
 */
public enum ChunkOrigin {
    COMPACTOR("compactor"), // Compactor permanent job
    INJECTOR("injector"), // Logisland injector job
    GATEWAY("gateway"); // Historian server (REST api server)

    private String value = null;

    private ChunkOrigin(String value) {
        this.value = value;
    }

    public String toString() {
        return value;
    }
}
