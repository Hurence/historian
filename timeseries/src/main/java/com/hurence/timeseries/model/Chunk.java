package com.hurence.timeseries.model;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.compaction.BinaryEncodingUtils;
import com.hurence.timeseries.converter.ChunkTruncater;
import lombok.*;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;

/**
 * A Chunk is a compressed version of a bulk of Measure.
 * <p>
 * The list of values is stored as a protobuf encoded byte array.
 * there are several pre-computed aggregations to help sampling and
 * big time scale high grained analytics
 *
 * @see Measure
 */
import java.util.Map;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;

/**
 * A Chunk is a compacted set of measures within a time interval.
 * The value is stored as a protobuf encoded chunk of data. If you expand this value you'll end up with a set of
 * Measures. The quality is stored within the value itself
 * Some aggregations and meta-information is stored along this data structure in order to facilitate analytics
 * without uncompacting the binary value : avg, min, max, stdDev, first, last ...
 * the minimal parts of a Chunk are      : name, start, end, value
 * and eventually                        : tags
 *
 * @see Measure
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Chunk implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Chunk.class);

    @Builder.Default
    protected SchemaVersion version = SchemaVersion.VERSION_1;
    protected String name;
    protected byte[] value;
    protected long start;
    protected long end;
    protected long count;
    protected double first;
    protected double min;
    protected double max;
    protected double sum;
    protected double avg;
    protected double last;
    protected double stdDev;

    // agg quality
    @Builder.Default
    protected float qualityFirst = Float.NaN;
    @Builder.Default
    protected float qualityMin = Float.NaN;
    @Builder.Default
    protected float qualityMax = Float.NaN;
    @Builder.Default
    protected float qualitySum = Float.NaN;
    @Builder.Default
    protected float qualityAvg = Float.NaN;

    // meta
    protected int year;
    protected int month;
    protected String day;
    protected String origin;
    protected String sax;
    protected boolean trend;
    protected boolean outlier;
    protected String id;
    protected String metricKey;

    protected Map<String, String> tags;

    // Naming pattern <Class>Builder of this class will make lombok use this class
    // as the builder for Chunk. Said differently, an instance of this ChunkBuilder
    // class will be returned by Chunk.build().
    public static class ChunkBuilder {

        /**
         * sets id to an idempotent hash
         *
         * @return
         *
         * TODO: tried to have this method automatically with lombok but did not work:
         * Tried with @Builder.ObtainVia(method = "buildId") as annotation for id field
         * Further lombok documentation for the builder at:
         * https://www.projectlombok.org/features/Builder
         */
        public ChunkBuilder buildId() {
            StringBuilder newId = new StringBuilder();

            if (tags == null) {
                // Workaround to initializer not working with lombok:
                // If we declare tags like this:
                //   @Builder.Default
                //   protected Map<String, String> tags = new HashMap<String, String>();
                // We get the following compilation error:
                // java: non-static variable tags cannot be referenced from a static context
                tags = new HashMap<String, String>();
            }

            metricKey = buildMetricKey(); // Compute and store metric key

            newId.append(metricKey);
            try {
                newId.append(BinaryEncodingUtils.encode(value));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Error encoding binaries", e);
            }

            // Id is hash of metric key + measures
            id = Hashing.sha256()
                    .hashString(newId.toString(), StandardCharsets.UTF_8)
                    .toString();

            return this;
        }

        private String buildMetricKey() {
            return new MetricKey(name, tags).compute();
        }

        /**
         * compute the metrics from the valueBinaries field so ther's no need to erad them
         */
        public ChunkBuilder computeMetrics() {
            DateTime time = new DateTime(start)
                    .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC"))));
            day = time.toString("yyyy-MM-dd");
            year = time.getYear();
            month = time.getMonthOfYear();
            return this;
        }

    }

    public String toHumanReadable() {
        SimpleDateFormat sdf = Measure.createUtcDateFormatter("yyyy-MM-dd HH:mm:ss.SSS");
        StringBuilder stringBuilder = new StringBuilder(" Human readable value:");
        try {
            TreeSet<Measure> measures = BinaryCompactionUtil.unCompressPoints(value, start, end);
            for (Measure measure : measures) {
                double measureValue = measure.getValue();
                float quality = measure.getQuality();
                String readableTimestamp = sdf.format(new Date(measure.getTimestamp()));
                stringBuilder.append("\n    t=").append(readableTimestamp)
                        .append(" v=").append(measureValue)
                        .append(" q=").append(quality);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE) + stringBuilder;
    }

    /**
     * Facility class to compute and parse metric key
     */
    public static class MetricKey {

        private String name;
        private Map<String, String> tags = new HashMap<String, String>();

        /**
         * Constructor when no tags
         * @param name
         */
        private MetricKey (String name) {
            Objects.requireNonNull(name);
            this.name = name;
        }

        private MetricKey (String name, Map<String, String> tags) {
            Objects.requireNonNull(name);
            Objects.requireNonNull(tags);
            this.name = name;
            this.tags = tags;
        }

        /**
         * Computes the unique metric key in the form:
         * <name>[,tagName=tageValue] with tags alphabetically sorted
         * according to the tag name
         * @return
         */
        public String compute() {
            StringBuilder idBuilder = new StringBuilder(name);
            // If there are some tags, add them with their values in an alphabetically sorted way
            // according to the tag key
            if (tags.size() > 0) {
                SortedSet sortedTags = new TreeSet(tags.keySet()); // Sort tag keys
                sortedTags.forEach( (tagKey) -> {
                    idBuilder.append(",").append(tagKey).append("=").append(tags.get(tagKey));
                });
            }
            return idBuilder.toString();
        }

        @Override
        public String toString() {
            return compute();
        }

        /**
         * Parses a metric key in the form as defined by the compute method
         * @param id
         * @return
         */
        public static MetricKey parse(String id) {

            // metricName,tag1=tag1Val,tag2=tag2val
            String[] tokens = id.split(",");
            if ( (tokens == null) || (tokens.length == 0)) {
                throw new IllegalArgumentException("null or empty metric key");
            }
            String name = tokens[0];
            if (tokens.length == 1) {
                // No tags ( ["metricName"] )
                return new MetricKey(name);
            } else
            {
                // Some tags: parse them ( ["metricName", "tag1=tag1Val" , "tag2=tag2val"] )
                Map<String, String> tags = new HashMap<String, String>();
                for (int i=1 ; i < tokens.length ; i++) {
                    // "tag1=tag1Val"
                    String tagAndValues = tokens[i];
                    String[] tagAndValue = tagAndValues.split("=");
                    if ( (tagAndValue == null) || (tagAndValue.length != 2) ) {
                        throw new IllegalArgumentException("tag component has wrong format: " + tagAndValue);
                    }
                    tags.put(tagAndValue[0], tagAndValue[1]);
                }
                return new MetricKey(name, tags);
            }
        }

        public Set<String> getTagKeys() {
            return tags.keySet();
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getTags() {
            return tags;
        }
    }

    /**
     * Convert byte[] value field as byte64 String.
     *
     * @return the base64 encoded String
     */
    public String getValueAsString() {
        try {
            return BinaryEncodingUtils.encode(value);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }


    /**
     * Convert byte[] value field into a chrono-ordered set of Measures
     *
     * @return the set of Measures
     * @throws IOException
     */
    public TreeSet<Measure> getValueAsMeasures() throws IOException {
        return BinaryCompactionUtil.unCompressPoints(value, start, end);
    }

    public boolean containsTag(String tagName) {
        return tags.containsKey(tagName);
    }

    public String getTag(String tagName) {
        return tags.get(tagName);
    }


    /**
     * Cut a Chunk into a smaller time range
     *
     * @param from
     * @param to
     * @return a truncated copy of the Chunk
     * @see ChunkTruncater
     */
    public Chunk truncate(long from, long to) {
        try {
            return ChunkTruncater.truncate(this, from, to);
        } catch (IOException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

}