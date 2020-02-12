package com.hurence.webapiservice.http.grafana.modele;

import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;

import java.util.List;

public class AnnotationRequestParam implements TimeSeriesRequest {

    private List<String> tags;
    private long from;
    private long to;
    private long fromRaw;
    private long toRaw;
    private int maxAnnotation;
    private Boolean matchAny;
    private String type;


    private AnnotationRequestParam() { }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public void setFromRaw(long fromRaw) {
        this.fromRaw = fromRaw;
    }

    public void setToRaw(long toRaw) {
        this.toRaw = toRaw;
    }

    public void setMaxAnnotation(int maxAnnotation) {
        this.maxAnnotation = maxAnnotation;
    }

    public void setMatchAny(Boolean matchAny) {
        this.matchAny = matchAny;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getTags() {
        return tags;
    }

    public long getFrom() {
        return from;
    }

    @Override
    public List<AGG> getAggs() {
        return null;
    }

    public long getTo() {
        return to;
    }

    @Override
    public SamplingConf getSamplingConf() {
        return null;
    }

    @Override
    public List<String> getMetricNames() {
        return null;
    }

    public long getFromRaw() {
        return fromRaw;
    }

    public long getToRaw() {
        return toRaw;
    }

    public int getMaxAnnotation() {
        return maxAnnotation;
    }

    public Boolean getMatchAny() {
        return matchAny;
    }

    public String getType() {
        return type;
    }


    public static final class Builder {
        private List<String> tags;
        private long from;
        private long to;
        private long fromRaw;
        private long toRaw;
        private int maxAnnotation;
        private Boolean matchAny;
        private String type;

        public Builder() { }

        public AnnotationRequestParam.Builder withTags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public AnnotationRequestParam.Builder from(long from) {
            this.from = from;
            return this;
        }

        public AnnotationRequestParam.Builder to(long to) {
            this.to = to;
            return this;
        }

        public AnnotationRequestParam.Builder fromRaw(long fromRaw) {
            this.fromRaw = fromRaw;
            return this;
        }

        public AnnotationRequestParam.Builder toRaw(long toRaw) {
            this.toRaw = toRaw;
            return this;
        }


        public AnnotationRequestParam.Builder withMaxAnnotation(int maxAnnotation) {
            this.maxAnnotation = maxAnnotation;
            return this;
        }

        public AnnotationRequestParam.Builder withMatchAny(Boolean matchany) {
            this.matchAny = matchAny;
            return this;
        }

        public AnnotationRequestParam.Builder withType(String type) {
            this.type = type;
            return this;
        }

        public AnnotationRequestParam build() {
            AnnotationRequestParam getAnnotationRequestParam = new AnnotationRequestParam();
            getAnnotationRequestParam.setTags(tags);
            getAnnotationRequestParam.setFrom(from);
            getAnnotationRequestParam.setTo(to);
            getAnnotationRequestParam.setFromRaw(fromRaw);
            getAnnotationRequestParam.setToRaw(toRaw);
            getAnnotationRequestParam.setMaxAnnotation(maxAnnotation);
            getAnnotationRequestParam.setMatchAny(matchAny);
            getAnnotationRequestParam.setType(type);
            return getAnnotationRequestParam;
        }
    }





}
