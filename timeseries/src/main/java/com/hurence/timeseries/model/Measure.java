package com.hurence.timeseries.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.time.ZoneId;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A Measure is a value in time with some tags
 * the comparability is based on the timestamps to sort them
 *
 * the minimal parts of a Measure are :  name, timestamp, value
 * and eventually                     :  tags, quality
 *
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



    public boolean containsTag(String tagName) {
        return tags.containsKey(tagName);
    }

    public String getTag(String tagName) {
        return tags.get(tagName);
    }


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

    public String getDay(){
        DateTime time = new DateTime(timestamp)
                .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC"))));
       return time.toString("yyyy-MM-dd");
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

        AtomicBoolean areTagsEquals = new AtomicBoolean(true);
        if(tags != null) {
            tags.keySet().forEach(key -> {
                String value = getTag(key);
                if (value != null && !value.isEmpty() && value.equals("null")) {
                    areTagsEquals.set(areTagsEquals.get() && value.equals(measure.getTag(key)));
                }
            });
        }

        boolean equality=  timestamp == measure.timestamp &&
                Double.compare(measure.value, value) == 0 &&
                Float.compare(measure.quality, quality) == 0 &&
                Objects.equals(name, measure.name) &&
                areTagsEquals.get();

        return equality;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timestamp, value, quality, tags);
    }

    @Override
    public int compareTo(Measure o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }
}