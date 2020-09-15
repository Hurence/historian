package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version1SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags extends AbstractSolrInjectorChunkCurrentVersion {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;
    private final List<Map<String, String>> tags;
    private final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("Version1SolrInjectorOneMetricMultipleChunksSpecificPoints");

    public Version1SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(String metricName,
                                                                             List<Map<String, String>> tags,
                                                                             List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
        this.tags = tags;
    }

    @Override
    protected List<ChunkVersionCurrent> buildListOfChunks() {
        List<ChunkVersionCurrent> chunks = IntStream
                .range(0, Math.min(tags.size(), pointsByChunk.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkVersionCurrent buildChunk(int index) {
        ChunkVersionCurrent chunk = converter.buildChunk(metricName,
                new TreeSet<>(pointsByChunk.get(index)),
                tags.get(index));
        return chunk;
    }
}
