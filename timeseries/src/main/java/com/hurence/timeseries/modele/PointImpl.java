package com.hurence.timeseries.modele;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class PointImpl implements Point {
    private long timestamp;
    private double value;

    /**
     * Constructs a pair
     *
     * @param timestamp - the timestamp
     * @param value     - the value
     */
    public PointImpl(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
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
    public float getQuality() {
        throw new IllegalArgumentException("Not implemented method for PointImpl.");
    }

    @Override
    public boolean hasQuality() {
        return false;
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
        PointImpl rhs = (PointImpl) obj;
        return new EqualsBuilder()
                .append(this.timestamp, rhs.timestamp)
                .append(this.value, rhs.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(timestamp)
                .append(value)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("timestamp", timestamp)
                .append("value", value)
                .toString();
    }
}
