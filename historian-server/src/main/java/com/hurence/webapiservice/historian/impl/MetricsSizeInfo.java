package com.hurence.webapiservice.historian.impl;

import com.hurence.webapiservice.timeseries.extractor.MetricRequest;

import java.util.Set;

public interface MetricsSizeInfo {

    Set<MetricRequest> getMetricRequests();

    MetricSizeInfo getMetricInfo(MetricRequest metric);

    long getTotalNumberOfPoints();

    //TODO cette méthode n'est utilisé que dans l'implémentation de MetricsSizeInfo. Elle ne devrait donc pas être
    // public et donc pas présente dans l'interface.
    // Mais il est vrai que ce nom est plus parlant que la méthode getTotalNumberOfChunksToReturn
    // Tu peux donc a la place supprimer getTotalNumberOfChunksToReturn
    long getTotalNumberOfChunks();

    long getTotalNumberOfChunksWithCorrectQuality();

    long getTotalNumberOfPointsWithCorrectQuality();

    long getTotalNumberOfChunksToReturn();

    long getTotalNumberOfPointsToReturn();

    boolean isEmpty();
}

