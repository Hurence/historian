package com.hurence.timeseries.converter;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;

import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class is not thread safe !
 */
public interface PointsToChunk {

    SchemaVersion getVersion();

    Chunk buildChunk(String name,
                     TreeSet<? extends Measure> points,
                            Map<String, String> tags);

    default Chunk buildChunk(String name,
                             TreeSet<? extends Measure> points) {
        return buildChunk(name, points, Collections.emptyMap());
    }

}
