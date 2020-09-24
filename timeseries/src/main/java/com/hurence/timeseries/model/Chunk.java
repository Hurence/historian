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
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Chunk implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Chunk.class);

    protected SchemaVersion version;
    protected String name;
    protected byte[] valueBinaries;
    protected long start;
    protected long end;
    protected long count;
    protected double first;
    protected double min;
    protected double max;
    protected double sum;
    protected double avg;
    protected double last;
    protected double std;

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
    protected String chunkOrigin;
    protected String sax;
    protected boolean trend;
    protected boolean outlier;
    protected String id;

    protected Map<String, String> tags;


    /**
     * custom builder
     */
    public static class ChunkBuilder {

        /**
         * sets id to an idempotent hash
         */
        public ChunkBuilder buildId() {
            StringBuilder newId = new StringBuilder();

            newId.append(name);
            tags.forEach((k, v) -> newId.append(k + v));

            try {
                newId.append(BinaryEncodingUtils.encode(valueBinaries));

            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Error encoding binaries", e);
            }

            id = Hashing.sha256()
                    .hashString(newId.toString(), StandardCharsets.UTF_8)
                    .toString();

            return this;
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
            return BinaryEncodingUtils.encode(valueBinaries);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] getValueAsBinary() {
        return valueBinaries;
    }

    public TreeSet<Measure> getValueAsMeasures() throws IOException {
        return BinaryCompactionUtil.unCompressPoints(valueBinaries, start, end);
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