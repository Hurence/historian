package com.hurence.timeseries.modele.chunk;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.compaction.BinaryEncodingUtils;
import com.hurence.timeseries.converter.ChunkTruncater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChunkVersionCurrentImpl implements ChunkVersionCurrent {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkVersionCurrentImpl.class);

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
    //agg quality
    protected float qualityFirst;
    protected float qualityMin;
    protected float qualityMax;
    protected float qualitySum;
    protected float qualityAvg;
    //meta
    protected int year;
    protected int month;
    protected String day;
    protected String chunkOrigin;
    protected String sax;
    protected boolean trend;
    protected boolean outlier;
    protected List<String> compactionRunnings;
    protected String id;

    protected Map<String, String> tags;

    protected ChunkVersionCurrentImpl(SchemaVersion version, String name,
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
            this.id = ChunkVersionCurrent.buildId(this);
        } else {
            this.id = id;
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public SchemaVersion getVersion() {
        return version;
    }

    @Override
    public String getSax() {
        return sax;
    }

    @Override
    public double getLast() {
        return last;
    }

    @Override
    public double getStddev() {
        return std;
    }

    @Override
    public List<String> getCompactionsRunning() {
        return compactionRunnings;
    }

    @Override
    public boolean getTrend() {
        return trend;
    }

    @Override
    public boolean getOutlier() {
        return outlier;
    }

    @Override
    public float getQualityMin() {
        return qualityMin;
    }

    @Override
    public float getQualityMax() {
        return qualityMax;
    }

    @Override
    public float getQualitySum() {
        return qualitySum;
    }

    @Override
    public float getQualityFirst() {
        return qualityFirst;
    }

    @Override
    public float getQualityAvg() {
        return qualityAvg;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getValueAsString() {
        try {
            return BinaryEncodingUtils.encode(valueBinaries);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public byte[] getValueAsBinary() {
        return valueBinaries;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getFirst() {
        return first;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public double getAvg() {
        return avg;
    }

    @Override
    public int getYear() {
        return year;
    }

    @Override
    public int getMonth() {
        return month;
    }

    @Override
    public String getDay() {
        return day;
    }

    @Override
    public String getOrigin() {
        return chunkOrigin;
    }

    @Override
    public boolean containsTag(String tagName) {
        return tags.containsKey(tagName);
    }

    @Override
    public String getTag(String tagName) {
        return tags.get(tagName);
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public ChunkVersionCurrent truncate(long from, long to) {
        try {
            return ChunkTruncater.truncate(this, from, to);
        } catch (IOException e) {
            LOGGER.error("Error encoding binaries", e);
            throw new IllegalArgumentException(e);
        }
    }

    public static class Builder {
        private SchemaVersion version;
        private String name;
        private byte[] valueBinaries;
        private long start;
        private long end;
        //agg
        private long count;
        private double first;
        private double min;
        private double max;
        private double sum;
        private double avg;
        //agg quality
        protected float qualityFirst;
        protected float qualityMin;
        protected float qualityMax;
        protected float qualitySum;
        protected float qualityAvg;
        private int year;
        private int month;
        private String day;
        private String chunkOrigin;
        private Map<String, String> tags;
        private double last;
        private double std;
        private String sax;
        private boolean trend;
        private boolean outlier;
        private List<String> compactionRunnings = new ArrayList<>();
        private String id;


        public Builder setVersion(SchemaVersion version) {
            this.version = version;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setValueBinaries(byte[] valueBinaries) {
            this.valueBinaries = valueBinaries;
            return this;
        }

        public Builder setStart(long start) {
            this.start = start;
            return this;
        }

        public Builder setEnd(long end) {
            this.end = end;
            return this;
        }

        public Builder setCount(long count) {
            this.count = count;
            return this;
        }

        public Builder setFirst(double first) {
            this.first = first;
            return this;
        }

        public Builder setMin(double min) {
            this.min = min;
            return this;
        }

        public Builder setMax(double max) {
            this.max = max;
            return this;
        }

        public Builder setSum(double sum) {
            this.sum = sum;
            return this;
        }

        public Builder setAvg(double avg) {
            this.avg = avg;
            return this;
        }

        public Builder setQualityFirst(float qualityFirst) {
            this.qualityFirst = qualityFirst;
            return this;
        }

        public Builder setualityMin(float qualityMin) {
            this.qualityMin = qualityMin;
            return this;
        }

        public Builder setualityMax(float qualityMax) {
            this.qualityMax = qualityMax;
            return this;
        }

        public Builder setualitySum(float qualitySum) {
            this.qualitySum = qualitySum;
            return this;
        }

        public Builder setualityAvg(float qualityAvg) {
            this.qualityAvg = qualityAvg;
            return this;
        }

        public Builder setYear(int year) {
            this.year = year;
            return this;
        }

        public Builder setMonth(int month) {
            this.month = month;
            return this;
        }

        public Builder setDay(String day) {
            this.day = day;
            return this;
        }

        public Builder setTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public ChunkVersionCurrentImpl build() {
            return new ChunkVersionCurrentImpl(version, name,
                    valueBinaries, start, end,
                    count, first, min, max, sum, avg,
                    year, month, day, tags, chunkOrigin,
                    last, std, sax, trend, outlier,
                    compactionRunnings, id, qualityFirst, qualityMin,
                    qualityMax, qualitySum, qualityAvg);
        }

        public Builder setChunkOrigin(String chunkOrigin) {
            this.chunkOrigin = chunkOrigin;
            return this;
        }

        public Builder setLast(double last) {
            this.last = last;
            return this;
        }

        public Builder setStd(double std) {
            this.std = std;
            return this;
        }

        public Builder setSax(String sax) {
            this.sax = sax;
            return this;
        }

        public Builder setTrend(boolean trend) {
            this.trend = trend;
            return this;
        }

        public Builder setOutlier(boolean outlier) {
            this.outlier = outlier;
            return this;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setCompactionRunnings(List<String> compactionRunnings) {
            this.compactionRunnings = compactionRunnings;
            return this;
        }
    }
}

