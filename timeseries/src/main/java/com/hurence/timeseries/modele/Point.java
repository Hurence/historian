package com.hurence.timeseries.modele;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Point {
    private int index;
    private long timestamp;
    private double value;

    /**
     * Constructs a pair
     *
     * @param index     - the index of timestamp / value within the metric time series
     * @param timestamp - the timestamp
     * @param value     - the value
     */
    public Point(int index, long timestamp, double value) {
        this.index = index;
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the value
     */
    public double getValue() {
        return value;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        Point rhs = (Point) obj;
        return new EqualsBuilder()
                .append(this.index, rhs.index)
                .append(this.timestamp, rhs.timestamp)
                .append(this.value, rhs.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(index)
                .append(timestamp)
                .append(value)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("index", index)
                .append("timestamp", timestamp)
                .append("value", value)
                .toString();
    }
}
