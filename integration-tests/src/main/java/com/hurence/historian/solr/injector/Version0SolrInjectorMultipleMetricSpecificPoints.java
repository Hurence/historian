package com.hurence.historian.solr.injector;

import com.hurence.historian.spark.compactor.job.ChunkModeleVersion0;
import com.hurence.timeseries.modele.Point;
import com.hurence.timeseries.modele.PointImpl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Version0SolrInjectorMultipleMetricSpecificPoints extends AbstractVersion0SolrInjector {

    private final List<String> metricNames;
    private final List<List<Point>> pointsByMetric;

    public Version0SolrInjectorMultipleMetricSpecificPoints(List<String> metricNames,
                                                            List<List<Point>> pointsByMetric) {
        this.metricNames = metricNames;
        this.pointsByMetric = pointsByMetric;
    }

    @Override
    protected List<ChunkModeleVersion0> buildListOfChunks() {
        List<ChunkModeleVersion0> chunks = IntStream
                .range(0, Math.min(metricNames.size(), pointsByMetric.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModeleVersion0 buildChunk(int index) {
        return ChunkModeleVersion0.fromPoints(metricNames.get(index), pointsByMetric.get(index));
    }
}
