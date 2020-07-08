package com.hurence.timeseries.modele;

import java.io.Serializable;

public interface Point extends Comparable<Point>, Serializable {

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

    @Override
    default int compareTo(Point o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }
}
