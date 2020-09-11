package com.hurence.webapiservice.historian.models;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

import java.util.Set;

public interface MetricsSizeInfo {

    public Set<MetricRequest> getMetricRequests();

    public MetricSizeInfo getMetricInfo(MetricRequest metric);

    public long getTotalNumberOfPoints();

    public long getTotalNumberOfChunks();

    boolean isEmpty();
}

