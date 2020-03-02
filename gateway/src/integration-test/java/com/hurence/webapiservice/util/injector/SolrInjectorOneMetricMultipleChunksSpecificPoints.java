package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModeleForTest;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorOneMetricMultipleChunksSpecificPoints extends AbstractSolrInjector {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;

    public SolrInjectorOneMetricMultipleChunksSpecificPoints(String metricName,
                                                             List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
    }

    @Override
    protected List<ChunkModeleForTest> buildListOfChunks() {
        List<ChunkModeleForTest> chunks = IntStream
                .range(0, pointsByChunk.size())
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleForTest buildChunk(int index) {
        return ChunkModeleForTest.fromPoints(metricName, pointsByChunk.get(index));
    }
}

