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
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


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
    protected float qualityFirst = Float.NaN;
    protected float qualityMin;
    protected float qualityMax  = Float.NaN;
    protected float qualitySum;
    protected float qualityAvg;

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


    public static class ChunkBuilder {



        /**
         * sets id to an idempotent hash
         *
         * @return
         */
        public ChunkBuilder buildId() {
            String toHash = name +
                    start +
                    chunkOrigin;
            try {
                toHash += BinaryEncodingUtils.encode(valueBinaries) ;
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Error encoding binaries", e);
            }

            id = Hashing.sha256()
                    .hashString(toHash, StandardCharsets.UTF_8)
                    .toString();

            return this;
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
            return BinaryEncodingUtils.encode(valueBinaries);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] getValueAsBinary() {
        return valueBinaries;
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