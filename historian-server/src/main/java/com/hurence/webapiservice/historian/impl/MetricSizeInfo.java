package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

public class MetricSizeInfo {

    public MetricRequest metricRequest;
    public long totalNumberOfPoints;
    public long totalNumberOfChunks;
    public long totalNumberOfPointsWithCorrectQuality;
    public long totalNumberOfChunksWithCorrectQuality;

    @Override
    public String toString() {
        return "MetricSizeInfo{" +
                "metricRequest='" + metricRequest + '\'' +
                ", totalNumberOfPoints=" + totalNumberOfPoints +
                ", totalNumberOfChunks=" + totalNumberOfChunks +
                ", totalNumberOfPointsOfChunksWithCorrectQuality=" + totalNumberOfPointsWithCorrectQuality +
                ", totalNumberOfChunksWithCorrectQuality=" + totalNumberOfChunksWithCorrectQuality +
                '}';
    }

    public long totalNumberOfPointsToReturn(){
        if (metricRequest.getQuality().getQualityValue().isNaN())
            return totalNumberOfPoints;
        else
            return totalNumberOfPointsWithCorrectQuality;
    }

    public long totalNumberOfChunksToReturn(){
        if (metricRequest.getQuality().getQualityValue().isNaN())
            return totalNumberOfChunks;
        else
            return totalNumberOfChunksWithCorrectQuality;
    }
}
