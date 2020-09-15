package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion extends AbstractSolrInjectorChunkCurrentVersion {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;
    private final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("Version1SolrInjectorOneMetricMultipleChunksSpecificPoints");

    public SolrInjectorOneMetricMultipleChunksSpecificPointsChunkCurrentVersion(String metricName,
                                                                                List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
    }

    @Override
    protected List<ChunkVersionCurrent> buildListOfChunks() {
        List<ChunkVersionCurrent> chunks = IntStream
                .range(0, pointsByChunk.size())
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkVersionCurrent buildChunk(int index) {
        return converter.buildChunk(metricName, new TreeSet<>(pointsByChunk.get(index)));
    }
}
