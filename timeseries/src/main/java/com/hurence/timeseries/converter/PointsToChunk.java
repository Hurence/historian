package com.hurence.timeseries.converter;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.modele.chunk.Chunk;
import com.hurence.timeseries.modele.points.Point;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * This class is not thread safe !
 */
public interface PointsToChunk {

    SchemaVersion getVersion();

    Chunk buildChunk(String name,
                     TreeSet<? extends Point> points,
                            Map<String, String> tags);

    default Chunk buildChunk(String name,
                             TreeSet<? extends Point> points) {
        return buildChunk(name, points, Collections.emptyMap());
    }

}
