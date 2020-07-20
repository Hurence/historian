package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion1;
import com.hurence.timeseries.modele.Point;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version1SolrInjectorOneMetricMultipleChunksSpecificPoints extends AbstractVersion1SolrInjector{

    private final String metricName;
    private final List<List<Point>> pointsByChunk;

    public Version1SolrInjectorOneMetricMultipleChunksSpecificPoints(String metricName,
                                                                     List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
    }

    @Override
    protected List<ChunkModeleVersion1> buildListOfChunks() {
        List<ChunkModeleVersion1> chunks = IntStream
                .range(0, pointsByChunk.size())
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleVersion1 buildChunk(int index) {
        return ChunkModeleVersion1.fromPoints(metricName, pointsByChunk.get(index));
    }
}
