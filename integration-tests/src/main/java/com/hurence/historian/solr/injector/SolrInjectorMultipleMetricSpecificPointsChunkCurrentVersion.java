package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion extends AbstractSolrInjectorChunkCurrentVersion {

    private final List<String> metricNames;
    private final List<List<Measure>> pointsByMetric;
    private final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion");

    public SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion(List<String> metricNames,
                                                                       List<List<Measure>> pointsByMetric) {
        this.metricNames = metricNames;
        this.pointsByMetric = pointsByMetric;
    }
    @Override
    protected List<Chunk> buildListOfChunks() {
        List<Chunk> chunks = IntStream
                .range(0, Math.min(metricNames.size(), pointsByMetric.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private Chunk buildChunk(int index) {
        return converter.buildChunk(metricNames.get(index), new TreeSet<>(pointsByMetric.get(index)));
    }
}
