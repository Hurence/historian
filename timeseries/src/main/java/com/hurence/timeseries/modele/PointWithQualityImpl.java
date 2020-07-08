package com.hurence.timeseries.modele;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class PointWithQualityImpl implements Point {
    private long timestamp;
    private double value;
    private float quality;

    /**
     * Constructs a pair
     *
     * @param timestamp - the timestamp
     * @param value     - the value
     * @param quality   - the quality
     */
    public PointWithQualityImpl(long timestamp, double value, float quality) {
        this.timestamp = timestamp;
        this.value = value;
        this.quality = quality;
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
        return quality;
    }

    @Override
    public boolean hasQuality() {
        return true;
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
        PointWithQualityImpl rhs = (PointWithQualityImpl) obj;
        return new EqualsBuilder()
                .append(this.quality, rhs.quality)
                .append(this.timestamp, rhs.timestamp)
                .append(this.value, rhs.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(quality)
                .append(timestamp)
                .append(value)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("timestamp", timestamp)
                .append("value", value)
                .append("quality", quality)
                .toString();
    }
}
