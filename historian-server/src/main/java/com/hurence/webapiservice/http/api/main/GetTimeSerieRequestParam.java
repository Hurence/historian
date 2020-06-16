package com.hurence.webapiservice.http.api.main;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GetTimeSerieRequestParam {

    public static final int DEFAULT_BUCKET_SIZE = 1;//will be recomputed later by historian if necessary depending on maxDataPoints
    public static final SamplingAlgorithm DEFAULT_SAMPLING_ALGORITHM = SamplingAlgorithm.AVERAGE;
    public static final long DEFAULT_FROM = 0L;
    public static final long DEFAULT_TO = Long.MAX_VALUE;
    public static final int DEFAULT_MAX_DATAPOINTS = 1000;
    public static final  Map<String,String> DEFAULT_TAGS = Collections.emptyMap();
    public static final  List<AGG> DEFAULT_AGGS = Collections.emptyList();


    private List<String> names;
    private long from;
    private long to;
    private List<AGG> aggs;
    private int maxDataPoints;
    private int bucketSize;
    private SamplingAlgorithm samplingAlgo;
    private Map<String, String> tags;

    private GetTimeSerieRequestParam() { }

    public List<String> getMetricNames() {
        return names;
    }

    private void setNames(List<String> names) {
        this.names = names;
    }

    public long getFrom() {
        return from;
    }

    private void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }


    public SamplingConf getSamplingConf() {
        return new SamplingConf(this.samplingAlgo, this.bucketSize, this.maxDataPoints);
    }

    private void setTo(long to) {
        this.to = to;
    }

    public List<AGG> getAggs() {
        return aggs;
    }

    private void setAggs(List<AGG> aggs) {
        this.aggs = aggs;
    }

    public int getMaxDataPoints() {
        return maxDataPoints;
    }

    private void setMaxDataPoints(int maxDataPoints) {
        this.maxDataPoints = maxDataPoints;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    private void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public SamplingAlgorithm getSamplingAlgo() {
        return samplingAlgo;
    }

    private void setSamplingAlgo(SamplingAlgorithm samplingAlgo) {
        this.samplingAlgo = samplingAlgo;
    }

    public Map<String,String> getTags() {
        return tags;
    }


    public String getRequestId() {
        return "not defined";
    }

    private void setTags(Map<String,String> tags) {
        this.tags = tags;
    }

    public static final class Builder {
        private List<String> names;
        private long from;
        private long to;
        private List<AGG> aggs;
        private int maxDataPoints;
        private int bucketSize;
        private SamplingAlgorithm samplingAlgo;
        private Map<String, String> tags;

        public Builder() { }

        public Builder withMetricNames(List<String> names) {
            this.names = names;
            return this;
        }

        public Builder from(long from) {
            this.from = from;
            return this;
        }

        public Builder to(long to) {
            this.to = to;
            return this;
        }

        public Builder withAggs(List<AGG> aggs) {
            this.aggs = aggs;
            return this;
        }

        public Builder withMaxDataPoints(int maxDataPoints) {
            this.maxDataPoints = maxDataPoints;
            return this;
        }

        public Builder withBucketSize(int bucketSize) {
            this.bucketSize = bucketSize;
            return this;
        }

        public Builder withSamplingAlgo(SamplingAlgorithm samplingAlgo) {
            this.samplingAlgo = samplingAlgo;
            return this;
        }

        public Builder withTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public GetTimeSerieRequestParam build() {
            GetTimeSerieRequestParam getTimeSerieRequestParam = new GetTimeSerieRequestParam();
            getTimeSerieRequestParam.setNames(names);
            getTimeSerieRequestParam.setFrom(from);
            getTimeSerieRequestParam.setTo(to);
            getTimeSerieRequestParam.setAggs(aggs);
            getTimeSerieRequestParam.setMaxDataPoints(maxDataPoints);
            getTimeSerieRequestParam.setBucketSize(bucketSize);
            getTimeSerieRequestParam.setSamplingAlgo(samplingAlgo);
            getTimeSerieRequestParam.setTags(tags);
            return getTimeSerieRequestParam;
        }
    }
}

