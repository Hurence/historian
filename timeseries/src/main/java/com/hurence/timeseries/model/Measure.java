package com.hurence.timeseries.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;


/**
 * A Measure is a value in time with som tags
 * the comparability is based on the timestamps to sort them
 * @see Chunk
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Measure implements Serializable, Comparable<Measure> {
    public static final Float DEFAULT_QUALITY = Float.NaN;
    private String name;
    private long timestamp;
    private double value;
    @Builder.Default private float quality = DEFAULT_QUALITY;
    protected Map<String, String> tags;
    protected String day;


    public static Measure fromValueAndQuality(long timestamp, double value, float quality) {
        return Measure.builder()
                .timestamp(timestamp)
                .value(value)
                .quality(quality)
                .compute()
                .build();
    }

    public static Measure fromValue(long timestamp, double value) {
        return Measure.builder()
                .timestamp(timestamp)
                .value(value)
                .quality(Float.NaN)
                .compute()
                .build();
    }

    /**
     * Custom builder methods
     */
    public static class MeasureBuilder {

        public MeasureBuilder compute() {

            DateTime time = new DateTime(timestamp)
                    .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC"))));
            day = time.toString("yyyy-MM-dd");


            return this;
        }
    }


    /**
     * @return true if quality is set to something else than NaN
     */
    public boolean hasQuality() {
        return !Float.isNaN(quality);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measure measure = (Measure) o;
        return timestamp == measure.timestamp &&
                Double.compare(measure.value, value) == 0 &&
                Float.compare(measure.quality, quality) == 0 &&
                Objects.equals(name, measure.name) &&
                Objects.equals(tags, measure.tags) &&
                Objects.equals(day, measure.day);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timestamp, value, quality, tags, day);
    }

    @Override
    public int compareTo(Measure o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }
}