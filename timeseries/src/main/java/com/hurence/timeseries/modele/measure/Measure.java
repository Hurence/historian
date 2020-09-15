package com.hurence.timeseries.modele.measure;

public interface Measure {
    String getName();
    double getValue();
    long getTimestamp();
}

