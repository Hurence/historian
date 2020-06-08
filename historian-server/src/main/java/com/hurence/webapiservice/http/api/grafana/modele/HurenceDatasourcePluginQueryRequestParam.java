package com.hurence.webapiservice.http.api.grafana.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;

import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.modele.AGG.*;

public class HurenceDatasourcePluginQueryRequestParam implements TimeSeriesRequest {

    public static final int DEFAULT_BUCKET_SIZE = 1;//will be recomputed later by historian if necessary depending on maxDataPoints
    public static final SamplingAlgorithm DEFAULT_SAMPLING_ALGORITHM = SamplingAlgorithm.AVERAGE;
    public static final long DEFAULT_FROM = 0L;
    public static final long DEFAULT_TO = Long.MAX_VALUE;
    public static final int DEFAULT_MAX_DATAPOINTS = 1000;
    public static final String DEFAULT_FORMAT = "json";
    public static final String DEFAULT_REQUEST_ID = "Not defined";
    public static final Map<String, String> DEFAULT_TAGS = Collections.emptyMap();
    public static final List DEFAULT_AGGREGATION = Collections.EMPTY_LIST;
    public static final List<AGG> DEFAULT_ALL_AGGREGATION_LIST = Arrays.asList(values());

    private final List<String> metricNames;
    private final long from;
    private final long to;
    private final String format;
    private final int maxDataPoints;
    private final SamplingAlgorithm samplingAlgo;
    private final int bucketSize;
    private final Map<String, String> tags;
    private final String requestId;
    private final List<AGG> aggregList;

    private HurenceDatasourcePluginQueryRequestParam(List<String> metricNames, long from, long to, String format,
                                                     int maxDataPoints, SamplingAlgorithm samplingAlgo, int bucketSize,
                                                 Map<String, String> tags, String requestId, List<AGG> aggregList) {
        Objects.requireNonNull(metricNames);
        if (metricNames.isEmpty()) throw new IllegalArgumentException("metricNames should not be empty !");
        this.metricNames = metricNames;
        this.from = from;
        this.to = to;
        this.format = format;
        this.maxDataPoints = maxDataPoints;
        this.samplingAlgo = samplingAlgo;
        this.bucketSize = bucketSize;
        this.tags = tags;
        this.requestId = requestId;
        this.aggregList = aggregList;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public List<AGG> getAggs() {
        return aggregList;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public SamplingConf getSamplingConf() {
        return new SamplingConf(samplingAlgo, bucketSize, maxDataPoints);
    }

    @Override
    public List<String> getMetricNames() {
        return metricNames;
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String getRequestId() {
        return requestId;
    }

    public static class Builder {
        private List<String> metricNames;
        private long from = DEFAULT_FROM;
        private long to = DEFAULT_TO;
        private String format = DEFAULT_FORMAT;
        private int maxDataPoints = DEFAULT_MAX_DATAPOINTS;
        private SamplingAlgorithm samplingAlgo = DEFAULT_SAMPLING_ALGORITHM;
        private int bucketSize = DEFAULT_BUCKET_SIZE;
        private Map<String, String> tags = DEFAULT_TAGS;
        private String requestId = DEFAULT_REQUEST_ID;
        private List<AGG> aggreg = DEFAULT_AGGREGATION;

        public Builder withMetricNames(List<String> metricNames) {
            this.metricNames = metricNames;
            return this;
        }

        public Builder withFrom(long from) {
            this.from = from;
            return this;
        }

        public Builder withTo(long to) {
            this.to = to;
            return this;
        }

        public Builder withFormat(String format) {
            this.format = format;
            return this;
        }

        public Builder withMaxDataPoints(int maxDataPoints) {
            this.maxDataPoints = maxDataPoints;
            return this;
        }

        public Builder withSamplingAlgo(SamplingAlgorithm samplingAlgo) {
            this.samplingAlgo = samplingAlgo;
            return this;
        }

        public Builder withBucketSize(int bucketSize) {
            this.bucketSize = bucketSize;
            return this;
        }

        public Builder withTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder withRequestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public HurenceDatasourcePluginQueryRequestParam build() {
            return new HurenceDatasourcePluginQueryRequestParam(metricNames, from, to, format, maxDataPoints,
                    samplingAlgo, bucketSize, tags, requestId, aggreg);
        }

        public void withAggreg(List<AGG> aggreg) {
            this.aggreg = aggreg;
        }
    }
}
