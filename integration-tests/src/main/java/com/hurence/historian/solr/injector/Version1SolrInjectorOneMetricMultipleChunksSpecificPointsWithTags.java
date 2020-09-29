package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;


import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version1SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags extends AbstractSolrInjectorChunkCurrentVersion {

    private final String metricName;
    private final List<List<Measure>> measuresByChunk;
    private final List<Map<String, String>> tags;
    private final MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent("Version1SolrInjectorOneMetricMultipleChunksSpecificPoints");

    public Version1SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(String metricName,
                                                                             List<Map<String, String>> tags,
                                                                             List<List<Measure>> measuresByChunk) {
        this.metricName = metricName;
        this.measuresByChunk = measuresByChunk;
        this.tags = tags;
    }

    @Override
    protected List<Chunk> buildListOfChunks() {
        List<Chunk> chunks = IntStream
                .range(0, Math.min(tags.size(), measuresByChunk.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private Chunk buildChunk(int index) {
        Chunk chunk = converter.buildChunk(metricName,
                new TreeSet<>(measuresByChunk.get(index)),
                tags.get(index));
        return chunk;
    }
}
