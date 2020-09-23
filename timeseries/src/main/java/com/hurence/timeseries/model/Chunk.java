package com.hurence.timeseries.model;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.SchemaVersion;
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
    protected double std_dev;

    // agg quality
    protected float quality_first = Float.NaN;
    protected float quality_min;
    protected float quality_max  = Float.NaN;
    protected float quality_sum;
    protected float quality_avg;

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
            String toHash = name +
                    start +
                    origin;
            try {
                toHash += BinaryEncodingUtils.encode(value) ;
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Error encoding binaries", e);
            }

            id = Hashing.sha256()
                    .hashString(toHash, StandardCharsets.UTF_8)
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
         * compute the metrics from the valueBinaries field
         *
         * @return
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
/*
    protected Chunk(SchemaVersion version, String name,
                                      byte[] valueBinaries, long start, long end,
                                      long count, double first, double min,
                                      double max, double sum, double avg,
                                      int year, int month, String day,
                                      Map<String, String> tags,
                                      String chunkOrigin,
                                      double last,
                                      double std,
                                      String sax,
                                      boolean trend,
                                      boolean outlier,
                                      List<String> compactionRunnings,
                                      String id,
                                      float qualityFirst, float qualityMin,
                                      float qualityMax, float qualitySum, float qualityAvg) {
        this.version = version;
        this.name = name;
        this.valueBinaries = valueBinaries;
        this.start = start;
        this.end = end;
        this.count = count;
        this.first = first;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.avg = avg;
        this.qualityFirst = qualityFirst;
        this.qualityMin = qualityMin;
        this.qualityMax = qualityMax;
        this.qualitySum = qualitySum;
        this.qualityAvg = qualityAvg;
        this.year = year;
        this.month = month;
        this.day = day;
        this.tags = tags;
        this.chunkOrigin = chunkOrigin;
        this.last = last;
        this.std = std;
        this.sax = sax;
        this.trend = trend;
        this.outlier = outlier;
        this.compactionRunnings = compactionRunnings;
        if (id == null) {
            this.id = Chunk.buildId(this);
        } else {
            this.id = id;
        }
    }
*/

    public String getValueAsString() {
        try {
            return BinaryEncodingUtils.encode(value);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] getValueAsBinary() {
        return value;
    }

    public boolean containsTag(String tagName) {
        return tags.containsKey(tagName);
    }

    public String getTag(String tagName) {
        return tags.get(tagName);
    }

    public Chunk truncate(long from, long to) {
        try {
            return ChunkTruncater.truncate(this, from, to);
        } catch (IOException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

}