package com.hurence.historian.solr.injector;

import com.hurence.timeseries.converter.PointsToChunkVersionCurrent;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.Point;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion extends AbstractSolrInjectorChunkCurrentVersion {

    private final List<String> metricNames;
    private final List<List<Point>> pointsByMetric;
    private final PointsToChunkVersionCurrent converter = new PointsToChunkVersionCurrent("SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion");

    public SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion(List<String> metricNames,
                                                                       List<List<Point>> pointsByMetric) {
        this.metricNames = metricNames;
        this.pointsByMetric = pointsByMetric;
    }
    @Override
    protected List<ChunkVersionCurrent> buildListOfChunks() {
        List<ChunkVersionCurrent> chunks = IntStream
                .range(0, Math.min(metricNames.size(), pointsByMetric.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkVersionCurrent buildChunk(int index) {
        return converter.buildChunk(metricNames.get(index), new TreeSet<>(pointsByMetric.get(index)));
    }
}
