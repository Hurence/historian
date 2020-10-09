package com.hurence.historian.compactor;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import spire.random.rng.Serial;

import java.io.Serializable;

/**
 * Holds information about an old chunk to delete (after a recompaction cycle)
 */
public class ChunkToDelete implements Serializable {

    String id;
    String origin;

    public ChunkToDelete(String id, String origin) {
        this.id = id;
        this.origin = origin;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
