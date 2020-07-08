package com.hurence.timeseries.modele;

public interface Point {

    static Point fromValueAndQuality(long timestamp, double value, float quality) {
        return new PointWithQualityImpl(timestamp, value, quality);
    }

    static Point fromValue(long timestamp, double value) {
        return new PointImpl(timestamp, value);
    }

    long getTimestamp();
    double getValue();
    float getQuality();
    boolean hasQuality();
}
