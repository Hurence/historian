package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

public class MetricSizeInfo {

    public MetricRequest metricRequest;
    public long totalNumberOfPoints;
    public long totalNumberOfChunks;

    @Override
    public String toString() {
        return "MetricSizeInfo{" +
                "metricRequest='" + metricRequest + '\'' +
                ", totalNumberOfPoints=" + totalNumberOfPoints +
                ", totalNumberOfChunks=" + totalNumberOfChunks +
                '}';
    }
}
