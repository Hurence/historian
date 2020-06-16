package com.hurence.webapiservice.http.api.grafana.modele;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.GrafanaSimpleJsonPluginApiImpl;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.api.grafana.GrafanaSimpleJsonPluginApiImpl.FILTER_KEYS;

public class QueryRequestParam {

    public static final int DEFAULT_BUCKET_SIZE = 1;//will be recomputed later by historian if necessary depending on maxDataPoints
    public static final SamplingAlgorithm DEFAULT_SAMPLING_ALGORITHM = SamplingAlgorithm.AVERAGE;
    public static final long DEFAULT_FROM = 0L;
    public static final long DEFAULT_TO = Long.MAX_VALUE;
    public static final int DEFAULT_MAX_DATAPOINTS = 1000;
    public static final  List<AdHocFilter> DEFAULT_FILTERS = Collections.emptyList();

    private List<Target> targets;
    private long from;
    private long to;
    private String format;
    private int maxDataPoints;
    private List<AdHocFilter> adHocFilters;
    private String requestId;

    private QueryRequestParam() { }

    public List<Target> getTargets() {
        return targets;
    }

    private void setTargets(List<Target> targets) {
        this.targets = targets;
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

    private void setTo(long to) {
        this.to = to;
    }

    public String getFormat() {
        return format;
    }

    private void setFormat(String format) {
        this.format = format;
    }

    public int getMaxDataPoints() {
        return maxDataPoints;
    }

    private void setMaxDataPoints(int maxDataPoints) {
        this.maxDataPoints = maxDataPoints;
    }

    public List<AdHocFilter> getAdHocFilters() {
        if (adHocFilters == null) return Collections.emptyList();
        return adHocFilters;
    }

    private void setAdHocFilters(List<AdHocFilter> adHocFilters) {
        this.adHocFilters = adHocFilters;
    }

    public String getRequestId() {
        return requestId;
    }

    private void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<AGG> getAggs() {
        return Collections.emptyList();
    }

    public SamplingConf getSamplingConf() {
        if (containFilter(GrafanaSimpleJsonPluginApiImpl.ALGO_TAG_KEY) || containFilter(GrafanaSimpleJsonPluginApiImpl.BUCKET_SIZE_TAG_KEY)) {
            Optional<SamplingAlgorithm> algo = getAlgoFromFilter();
            Optional<Integer> bucketSize = getBucketSizeFromFilter();
            return new SamplingConf(
                    algo.orElse(SamplingAlgorithm.AVERAGE),
                    bucketSize.orElse(DEFAULT_BUCKET_SIZE),
                    getMaxDataPoints());
        } else {
            return new SamplingConf(DEFAULT_SAMPLING_ALGORITHM, DEFAULT_BUCKET_SIZE, getMaxDataPoints());
        }
    }

    private boolean containFilter(String tagKey) {
        return getAdHocFilters().stream()
                .anyMatch(adhoc -> tagKey.equals(adhoc.getKey()));
    }

    private Optional<Integer> getBucketSizeFromFilter() {
        return getAdHocFilters().stream()
                .filter(adhoc -> GrafanaSimpleJsonPluginApiImpl.BUCKET_SIZE_TAG_KEY.equals(adhoc.getKey()))
                .map(adhoc -> Integer.parseInt(adhoc.getValue()))
                .findAny();
    }

    private Optional<SamplingAlgorithm> getAlgoFromFilter() {
        return getAdHocFilters().stream()
                .filter(adhoc -> GrafanaSimpleJsonPluginApiImpl.ALGO_TAG_KEY.equals(adhoc.getKey()))
                .map(adhoc -> SamplingAlgorithm.valueOf(adhoc.getValue()))
                .findAny();
    }


    public List<String> getMetricNames() {
        return getTargets().stream()
                .map(Target::getTarget)
                .collect(Collectors.toList());
    }


    public Map<String, String> getTags() {
        return getAdHocFilters().stream()
                .filter(adhoc -> !FILTER_KEYS.contains(adhoc.getKey()))
                .collect(Collectors.toMap(AdHocFilter::getKey, AdHocFilter::getValue));
    }

    public static final class Builder {
        private List<Target> targets;
        private long from;
        private long to;
        private String format;
        private int maxDataPoints;
        private List<AdHocFilter> adHocFilters;
        private String requestId;

        public Builder() { }

        public Builder withTargets(List<Target> targets) {
            this.targets = targets;
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

        public Builder withFormat(String format) {
            this.format = format;
            return this;
        }

        public Builder withMaxDataPoints(int maxDataPoints) {
            this.maxDataPoints = maxDataPoints;
            return this;
        }

        public Builder withAdHocFilters(List<AdHocFilter> adHocFilters) {
            this.adHocFilters = adHocFilters;
            return this;
        }

        public Builder withId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public QueryRequestParam build() {
            QueryRequestParam getTimeSerieRequestParam = new QueryRequestParam();
            getTimeSerieRequestParam.setTargets(targets);
            getTimeSerieRequestParam.setFrom(from);
            getTimeSerieRequestParam.setTo(to);
            getTimeSerieRequestParam.setFormat(format);
            getTimeSerieRequestParam.setMaxDataPoints(maxDataPoints);
            getTimeSerieRequestParam.setAdHocFilters(adHocFilters);
            getTimeSerieRequestParam.setRequestId(requestId);
            return getTimeSerieRequestParam;
        }
    }
}
