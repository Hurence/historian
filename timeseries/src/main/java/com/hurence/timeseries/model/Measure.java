package com.hurence.timeseries.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.*;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Map;


@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Measure implements Serializable, Comparable<Measure> {
    private String name;
    private long timestamp;
    private double value;
    private float quality = Float.NaN;
    protected Map<String, String> tags;
    protected String day;


    public static Measure fromValueAndQuality(long timestamp, double value, float quality) {
        return Measure.builder()
                .timestamp(timestamp)
                .value(value)
                .quality(quality)
                .build();
    }

    public static Measure fromValue(long timestamp, double value) {
        return Measure.builder()
                .timestamp(timestamp)
                .value(value)
                .quality(Float.NaN)
                .build();
    }

    public static class MeasureBuilder {

        public Measure.MeasureBuilder compute() {


            DateTime time = new DateTime(timestamp);
            day = time.toString("yyyy-MM-dd");


            return this;
        }
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

    public float getQuality() {
        return quality;
    }

    public boolean hasQuality() {
        return !Float.isNaN(quality);
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
        Measure rhs = (Measure) obj;
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

    @Override
    public int compareTo(Measure o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }
}