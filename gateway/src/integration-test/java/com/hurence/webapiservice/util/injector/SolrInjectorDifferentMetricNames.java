package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModeleForTest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SolrInjectorDifferentMetricNames extends AbstractSolrInjector {

    private final int size;
    private final int numberOfChunkByMetric;

    public SolrInjectorDifferentMetricNames(int numberOfMetric, int numberOfChunkByMetric) {
        this.size = numberOfMetric;
        this.numberOfChunkByMetric = numberOfChunkByMetric;
    }

    @Override
    protected List<ChunkModeleForTest> buildListOfChunks() {
        List<ChunkModeleForTest> chunks = IntStream.range(0, this.size)
                .mapToObj(i -> "metric_" + i)
                .map(this::buildChunkWithMetricName)
                .flatMap(this::createMoreChunkForMetric)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleForTest buildChunkWithMetricName(String metricName) {
        return ChunkModeleForTest.fromPoints(metricName, Arrays.asList(
                new Point(0, 1L, 5),
                new Point(0, 2L, 8),
                new Point(0, 3L, 1.2),
                new Point(0, 4L, 6.5)
        ));
    }

    private Stream<ChunkModeleForTest> createMoreChunkForMetric(ChunkModeleForTest chunk) {
        List<ChunkModeleForTest> chunks = IntStream.range(0, this.numberOfChunkByMetric)
                .mapToObj(i -> {
                    //TODO eventually change chunk content if needed
                    ChunkModeleForTest cloned = chunk;
                    return cloned;
                })
                .collect(Collectors.toList());
        return chunks.stream();
    }
}
