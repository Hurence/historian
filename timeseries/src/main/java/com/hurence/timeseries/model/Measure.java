package com.hurence.timeseries.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.*;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;


@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Measure implements Serializable, Comparable<Measure> {
    public static final Float DEFAULT_QUALITY = Float.NaN;
    private String name;
    private long timestamp;
    private double value;
    private float quality = DEFAULT_QUALITY;
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
        return fromValueAndQuality(timestamp, value, Float.NaN);
    }

    /**
     * Creates a measure from a value, specifying a date UTC date string with the format:
     * "yyyy-MM-dd HH:mm:ss.SSS"
     * @param date
     * @param value
     * @return
     */
    public static Measure fromValueWithDate(String date, double value) {
        return fromValueAndQualityWithDate(date, "yyyy-MM-dd HH:mm:ss.SSS", value, Float.NaN);
    }

    /**
     * Creates a measure from a value and quality, specifying a date UTC date string with the format:
     * "yyyy-MM-dd HH:mm:ss.SSS"
     * @param date
     * @param value
     * @param quality
     * @return
     */
    public static Measure fromValueAndQualityWithDate(String date, double value, float quality) {
        return fromValueAndQualityWithDate(date, "yyyy-MM-dd HH:mm:ss.SSS", value, quality);
    }

    /**
     * Creates a Measure from the value and quality, specifying the date of the measure with a date string
     * and a pattern.
     * For instance using pattern "yyyy-MM-dd HH:mm:ss.SSS" and date "2020-08-28 17:48:53.253"
     * will result in 1598636933253 timestamp usage
     * Warning: the pattern must not include a zone (UTC is always assumed)
     * @param date
     * @param pattern
     * @param value
     * @param quality
     * @return
     */
    public static Measure fromValueAndQualityWithDate(String date, String pattern, double value, float quality) {
        SimpleDateFormat sdf = createUtcDateFormatter(pattern);
        Date parsedDate = null;
        try {
            parsedDate = sdf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }

        return fromValueAndQuality(parsedDate.getTime(), value, quality);
    }

    /**
     * Creates a simple date formatter able to parse the passed pattern.
     * Warning: the pattern must not contain the timezone as the UTC timezone is hardcoded and
     * mandatory (all historian measure timestamps should be UTC)
     * @param pattern
     * @return
     */
    private static SimpleDateFormat createUtcDateFormatter(String pattern) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(pattern);
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        dateFormatter.setLenient(false);
        return dateFormatter;
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