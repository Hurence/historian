package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.modele.Point;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version0SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags extends AbstractVersion0SolrInjector {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;
    private final List<Map<String, String>> tags;

    public Version0SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(String metricName,
                                                                             List<Map<String, String>> tags,
                                                                             List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
        this.tags = tags;
    }

    @Override
    protected List<ChunkModeleVersion0> buildListOfChunks() {
        List<ChunkModeleVersion0> chunks = IntStream
                .range(0, Math.min(tags.size(), pointsByChunk.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleVersion0 buildChunk(int index) {
        ChunkModeleVersion0 chunk = ChunkModeleVersion0.fromPoints(metricName, pointsByChunk.get(index));
        chunk.tagsAsKeyValue = tags.get(index);
        return chunk;
    }
}

