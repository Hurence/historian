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
    public long getTotalNumberOfChunksWithCorrectQuality() {
        return metricsInfo.values().stream().mapToLong(metricInfo -> metricInfo.totalNumberOfChunksWithCorrectQuality).sum();
    }

    @Override
    public long getTotalNumberOfPointsWithCorrectQuality() {
        return metricsInfo.values().stream().mapToLong(metricInfo -> metricInfo.totalNumberOfPointsWithCorrectQuality).sum();
    }

    @Override
    public boolean isEmpty() {
        return metricsInfo.isEmpty();
    }

    public void setMetricInfo(MetricSizeInfo metricInfo) {
        metricsInfo.put(metricInfo.metricRequest, metricInfo);
    }

    /**
     * increase the number of point of MetricSizeInfo corresponding to this metricRequest if it already exist otherwise add a
     * new MetricSizeInfo with this metricRequest initialized with numberOfPoints.
     * @param metric
     * @param numberOfPoints
     */
    public void increaseNumberOfPointsForMetricRequest(MetricRequest metric, long numberOfPoints) {
        if (metricsInfo.containsKey(metric)) {
            metricsInfo.get(metric).totalNumberOfPoints += numberOfPoints;
        } else {
            MetricSizeInfo metricInfo = new MetricSizeInfo();
            metricInfo.totalNumberOfPoints = numberOfPoints;
            metricInfo.metricRequest = metric;
            setMetricInfo(metricInfo);
        }
    }

    /**
     * increase the number of chunks of MetricSizeInfo corresponding to this metricRequest if it already exist otherwise add a
     * new MetricSizeInfo with this metricRequest initialized with numberOfChunks.
     * @param metric
     */
    public void increaseNumberOfChunksForMetricRequest(MetricRequest metric) {
        if (metricsInfo.containsKey(metric)) {
            metricsInfo.get(metric).totalNumberOfChunks += 1;
        } else {
            MetricSizeInfo metricInfo = new MetricSizeInfo();
            metricInfo.totalNumberOfChunks = 1;
            metricInfo.metricRequest = metric;
            setMetricInfo(metricInfo);
        }
    }

    /**
     * increase the number of chunks of MetricSizeInfo corresponding to this metricRequest if it already exist otherwise add a
     * new MetricSizeInfo with this metricRequest initialized with numberOfChunks.
     * @param metric
     * @param numberOfChunks
     */
    public void increaseNumberOfChunksForMetricRequest(MetricRequest metric, long numberOfChunks) {
        if (metricsInfo.containsKey(metric)) {
            metricsInfo.get(metric).totalNumberOfChunks += numberOfChunks;;
        } else {
            MetricSizeInfo metricInfo = new MetricSizeInfo();
            metricInfo.totalNumberOfChunks = numberOfChunks;;
            metricInfo.metricRequest = metric;
            setMetricInfo(metricInfo);
        }
    }

    public void increaseNumberOfChunksWithQualityOkForMetricRequest(MetricRequest metric, long numberOfChunksWithCorrectQuality) {
        metricsInfo.get(metric).totalNumberOfChunksWithCorrectQuality += numberOfChunksWithCorrectQuality;
    }

    public void increaseNumberOfPointsWithQualityOkForMetricRequest(MetricRequest metric, long numberOfPointsWithCorrectQuality) {
        metricsInfo.get(metric).totalNumberOfPointsWithCorrectQuality += numberOfPointsWithCorrectQuality;
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder("MetricsSizeInfoImpl{");
        metricsInfo.values().forEach(strBuilder::append);
        strBuilder.append("}");
        return strBuilder.toString();
    }

    //TODO cette mécanique devrait être externaliser je pense. MetricsSizeInfo ne contient que des informations
    // pas de la logique métier ou fonctionnel. De plus je trouve bizarre ce if else on en reparlera lors du merge avec master.
    @Override
    public long getTotalNumberOfChunksToReturn() {
        if (metricsInfo.keySet().stream().findAny().get().getQuality().getQualityValue().isNaN())
            return getTotalNumberOfChunks();
        else
            return getTotalNumberOfChunksWithCorrectQuality();
    }

    //TODO pareil
    @Override
    public long getTotalNumberOfPointsToReturn() {
        if (metricsInfo.keySet().stream().findAny().get().getQuality().getQualityValue().isNaN())
            return getTotalNumberOfPoints();
        else
            return getTotalNumberOfPointsWithCorrectQuality();
    }
}
