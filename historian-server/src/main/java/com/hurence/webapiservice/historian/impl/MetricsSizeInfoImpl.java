package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MetricsSizeInfoImpl implements MetricsSizeInfo {

    private Map<MetricRequest, MetricSizeInfo> metricsInfo = new HashMap<>();

    @Override
    public Set<MetricRequest> getMetricRequests() {
        return metricsInfo.keySet();
    }

    @Override
    public MetricSizeInfo getMetricInfo(MetricRequest metric) {
        return metricsInfo.get(metric);
    }

    @Override
    public long getTotalNumberOfPoints() {
        return metricsInfo.values().stream().mapToLong(metricInfo -> metricInfo.totalNumberOfPoints).sum();
    }

    @Override
    public long getTotalNumberOfChunks() {
        return metricsInfo.values().stream().mapToLong(metricInfo -> metricInfo.totalNumberOfChunks).sum();
    }

    @Override
    public boolean isEmpty() {
        return metricsInfo.isEmpty();
    }

    public void setMetricInfo(MetricSizeInfo metricInfo) {
        metricsInfo.put(metricInfo.metricRequest, metricInfo);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder("MetricsSizeInfoImpl{");
        metricsInfo.values().forEach(strBuilder::append);
        strBuilder.append("}");
        return strBuilder.toString();
    }
}
