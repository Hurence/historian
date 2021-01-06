package com.hurence.timeseries.converter;

import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;

import java.util.TreeSet;

public interface ChunkToMeasures {
    TreeSet<Measure> buildMeasures(Chunk chunk);
}
