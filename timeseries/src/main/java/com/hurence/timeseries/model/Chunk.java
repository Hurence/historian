package com.hurence.timeseries.model;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.compaction.BinaryCompactionUtil;
import com.hurence.timeseries.compaction.BinaryEncodingUtils;
import com.hurence.timeseries.converter.ChunkTruncater;
import lombok.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
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

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Chunk implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Chunk.class);

    protected SchemaVersion version;
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
    protected String metric_key;

    protected Map<String, String> tags;


    // Naming pattern <Class>Builder of this class will make lombok use this class
    // as the builder for Chunk. Ssaid differently, an instance of this ChunkBuilder
    // class will be returned by Chunk.build().
    /**
     * custom builder
     */
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

            newId.append(name);
            tags.forEach((k, v) -> newId.append(k + v));

            String toHash = name +
                    start +
                    origin;
            try {
                newId.append(BinaryEncodingUtils.encode(value));

                toHash += BinaryEncodingUtils.encode(value) ;
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Error encoding binaries", e);
            }

            id = Hashing.sha256()
                    .hashString(newId.toString(), StandardCharsets.UTF_8)
                    .toString();

            metric_key = buildMetricKey();

            return this;
        }

        private String buildMetricKey() {

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

    public String getValueAsString() {
        try {
            return BinaryEncodingUtils.encode(value);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }


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