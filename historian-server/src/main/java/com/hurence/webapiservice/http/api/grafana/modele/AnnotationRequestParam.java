package com.hurence.webapiservice.http.api.grafana.modele;

import com.hurence.webapiservice.timeseries.AnnotationRequest;

import java.util.Collections;
import java.util.List;

public class AnnotationRequestParam implements AnnotationRequest {

    public static final long DEFAULT_FROM = 0L;
    public static final long DEFAULT_TO = Long.MAX_VALUE;
    public static final boolean DEFAULT_MATCH_ANY = true;
    public static final List<String> DEFAULT_TAGS = Collections.emptyList();
    public static final int DEFAULT_MAX_ANNOTATION_TO_RETURN = 1000;
    public static final AnnotationRequestType DEFAULT_TYPE = AnnotationRequestType.ALL;

    private List<String> tags;
    private Long from;
    private Long to;
    private Integer maxAnnotation;
    private Boolean matchAny;
    private AnnotationRequestType type;


    private void setTags(List<String> tags) {
        this.tags = tags;
    }

    private void setFrom(Long from) {
        this.from = from;
    }

    private void setTo(Long to) {
        this.to = to;
    }

    private void setMaxAnnotation(int maxAnnotation) {
        this.maxAnnotation = maxAnnotation;
    }

    private void setMatchAny(Boolean matchAny) {
        this.matchAny = matchAny;
    }

    private void setType(AnnotationRequestType type) {
        this.type = type;
    }

    @Override
    public List<String> getTags() { // i need to use this getTags without the one in the TimeSeriesRquest
        return tags;
    }

    public Long getFrom() {
        return from;
    }

    public Long getTo() {
        return to;
    }

    @Override
    public int getMaxAnnotation() {
        return maxAnnotation;
    }

    @Override
    public boolean getMatchAny() {
        return matchAny;
    }

    @Override
    public AnnotationRequestType getType() {
        return type;
    }


    public static final class Builder {
        private List<String> tags;
        private Long from;
        private Long to;
        private Integer maxAnnotation;
        private Boolean matchAny;
        private AnnotationRequestType type;

        public Builder() { }

        public AnnotationRequestParam.Builder withTags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public AnnotationRequestParam.Builder from(Long from) {
            this.from = from;
            return this;
        }

        public AnnotationRequestParam.Builder to(Long to) {
            this.to = to;
            return this;
        }



        public AnnotationRequestParam.Builder withMaxAnnotation(Integer maxAnnotation) {
            this.maxAnnotation = maxAnnotation;
            return this;
        }

        public AnnotationRequestParam.Builder withMatchAny(Boolean matchAny) {
            this.matchAny = matchAny;
            return this;
        }

        public AnnotationRequestParam.Builder withType(AnnotationRequestType type) {
            this.type = type;
            return this;
        }

        public AnnotationRequestParam build() {
            AnnotationRequestParam getAnnotationRequestParam = new AnnotationRequestParam();
            getAnnotationRequestParam.setTags(tags);
            getAnnotationRequestParam.setFrom(from);
            getAnnotationRequestParam.setTo(to);
            getAnnotationRequestParam.setMaxAnnotation(maxAnnotation);
            getAnnotationRequestParam.setMatchAny(matchAny);
            getAnnotationRequestParam.setType(type);
            return getAnnotationRequestParam;
        }
    }





}

