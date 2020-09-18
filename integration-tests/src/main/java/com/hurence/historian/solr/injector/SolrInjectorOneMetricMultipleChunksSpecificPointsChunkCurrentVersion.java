package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion extends AbstractSolrInjectorChunkCurrentVersion {

    private final String metricName;
    private final List<List<Measure>> MeasuresByChunk;
    private final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("Version1SolrInjectorOneMetricMultipleChunksSpecificPoints");

    public SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(String metricName,
                                                                                List<List<Measure>> MeasuresByChunk) {
        this.metricName = metricName;
        this.MeasuresByChunk = MeasuresByChunk;
    }

    @Override
    protected List<Chunk> buildListOfChunks() {
        List<Chunk> chunks = IntStream
                .range(0, MeasuresByChunk.size())
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private Chunk buildChunk(int index) {
        return converter.buildChunk(metricName, new TreeSet<>(MeasuresByChunk.get(index)));
    }
}
