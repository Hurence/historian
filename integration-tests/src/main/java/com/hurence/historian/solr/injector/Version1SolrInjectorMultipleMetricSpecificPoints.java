package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion1;
import com.hurence.timeseries.modele.Point;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version1SolrInjectorMultipleMetricSpecificPoints extends AbstractVersion1SolrInjector{

    private final List<String> metricNames;
    private final List<List<Point>> pointsByMetric;

    public Version1SolrInjectorMultipleMetricSpecificPoints(List<String> metricNames,
                                                            List<List<Point>> pointsByMetric) {
        this.metricNames = metricNames;
        this.pointsByMetric = pointsByMetric;
    }

    @Override
    protected List<ChunkModeleVersion1> buildListOfChunks() {
        List<ChunkModeleVersion1> chunks = IntStream
                .range(0, Math.min(metricNames.size(), pointsByMetric.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleVersion1 buildChunk(int index) {
        return ChunkModeleVersion1.fromPoints(metricNames.get(index), pointsByMetric.get(index));
    }
}
