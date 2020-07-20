package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion1;
import com.hurence.timeseries.modele.Point;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version1SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags extends AbstractVersion1SolrInjector{

    private final String metricName;
    private final List<List<Point>> pointsByChunk;
    private final List<Map<String, String>> tags;

    public Version1SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(String metricName,
                                                                             List<Map<String, String>> tags,
                                                                             List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
        this.tags = tags;
    }

    @Override
    protected List<ChunkModeleVersion1> buildListOfChunks() {
        List<ChunkModeleVersion1> chunks = IntStream
                .range(0, Math.min(tags.size(), pointsByChunk.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleVersion1 buildChunk(int index) {
        ChunkModeleVersion1 chunk = ChunkModeleVersion1.fromPoints(metricName, pointsByChunk.get(index));
        chunk.tagsAsKeyValue = tags.get(index);
        return chunk;
    }
}
