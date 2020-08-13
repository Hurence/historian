package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

import java.util.Set;

public interface MetricsSizeInfo {

    public Set<MetricRequest> getMetricRequests();

    public MetricSizeInfo getMetricInfo(MetricRequest metric);

    public long getTotalNumberOfPoints();

    public long getTotalNumberOfChunks();

    public long getTotalNumberOfChunksWithCorrectQuality();

    public long getTotalNumberOfPointsWithCorrectQuality();

    long getTotalNumberOfChunksToReturn();

    long getTotalNumberOfPointsToReturn();

    boolean isEmpty();
}

