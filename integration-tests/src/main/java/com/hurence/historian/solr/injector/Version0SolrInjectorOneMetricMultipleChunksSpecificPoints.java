package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.modele.Point;
import com.hurence.timeseries.modele.PointImpl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version0SolrInjectorOneMetricMultipleChunksSpecificPoints extends AbstractVersion0SolrInjector {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;

    public Version0SolrInjectorOneMetricMultipleChunksSpecificPoints(String metricName,
                                                                     List<List<Point>> pointsByChunk) {
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

