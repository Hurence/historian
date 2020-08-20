package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

import java.util.Set;

public interface MetricsSizeInfo {

    Set<MetricRequest> getMetricRequests();

    MetricSizeInfo getMetricInfo(MetricRequest metric);

    long getTotalNumberOfPoints();

    long getTotalNumberOfChunks();

    long getTotalNumberOfChunksWithCorrectQuality();

    long getTotalNumberOfPointsWithCorrectQuality();

    boolean isEmpty();
}

