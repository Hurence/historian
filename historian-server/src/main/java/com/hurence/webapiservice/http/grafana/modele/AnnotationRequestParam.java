package com.hurence.webapiservice.http.grafana.modele;

import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.AnnotationRequest;
import io.vertx.core.json.JsonArray;

import java.util.List;

public class AnnotationRequestParam implements AnnotationRequest {

    private JsonArray tags;
    private Long from;
    private Long to;
    private int maxAnnotation;
    private Boolean matchAny;
    private String type;


    public void setTags(JsonArray tags) {
        this.tags = tags;
    }

    public void setFrom(Long from) {
        this.from = from;
    }

    public void setTo(Long to) {
        this.to = to;
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

    @Override
    public JsonArray getTagsAsJsonArray() { // i need to use this getTags without the one in the TimeSeriesRquest
        return tags;
    }

    public Long getFrom() {
        return from;
    }


    public Long getTo() {
        return to;
    }

    @Override
    public SamplingConf getSamplingConf() {
        return null;
    }


    public List<String> getTags() {
        return null;
    }


    @Override
    public int getMaxAnnotation() {
        return maxAnnotation;
    }

    @Override
    public Boolean getMatchAny() {
        return matchAny;
    }

    @Override
    public String getType() {
        return type;
    }


    public static final class Builder {
        private JsonArray tags;
        private Long from;
        private Long to;
        private int maxAnnotation;
        private Boolean matchAny;
        private String type;

        public Builder() { }

        public AnnotationRequestParam.Builder withTags(JsonArray tags) {
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



        public AnnotationRequestParam.Builder withMaxAnnotation(int maxAnnotation) {
            this.maxAnnotation = maxAnnotation;
            return this;
        }

        public AnnotationRequestParam.Builder withMatchAny(Boolean matchAny) {
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
            getAnnotationRequestParam.setMaxAnnotation(maxAnnotation);
            getAnnotationRequestParam.setMatchAny(matchAny);
            getAnnotationRequestParam.setType(type);
            return getAnnotationRequestParam;
        }
    }





}
