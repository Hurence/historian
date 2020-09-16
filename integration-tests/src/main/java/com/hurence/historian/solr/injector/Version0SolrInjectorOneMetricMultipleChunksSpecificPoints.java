package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.model.Measure;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version0SolrInjectorOneMetricMultipleChunksSpecificPoints extends AbstractVersion0SolrInjector {

    private final String metricName;
    private final List<List<Measure>> pointsByChunk;

    public static Version0SolrInjectorOneMetricMultipleChunksSpecificPoints
        fromMetricsAndPoints(String metricName,
                                                                                               List<List<Measure>> pointsByChunk) {
        List<List<Measure>> pointsByChunkConverted = pointsByChunk.stream()
                .map(points -> {
                    return points.stream()
                            .map(p -> Measure.fromValue(p.getTimestamp(), p.getValue()))
                            .collect(Collectors.toList());
                })
                .collect(Collectors.toList());
        return new Version0SolrInjectorOneMetricMultipleChunksSpecificPoints(metricName, pointsByChunkConverted);
    }

    public Version0SolrInjectorOneMetricMultipleChunksSpecificPoints(String metricName,
                                                                     List<List<Measure>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
    }

    @Override
    protected List<ChunkModeleVersion0> buildListOfChunks() {
        List<ChunkModeleVersion0> chunks = IntStream
                .range(0, pointsByChunk.size())
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleVersion0 buildChunk(int index) {
        return ChunkModeleVersion0.fromPoints(metricName, pointsByChunk.get(index));
    }
}

