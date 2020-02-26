package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;

import java.util.List;

public interface AnnotationRequest {
    Long getFrom();

    Long getTo();


    SamplingConf getSamplingConf();

    String getType();

    List<String> getTags();

    JsonArray getTagsAsJsonArray();

    int getMaxAnnotation();

    Boolean getMatchAny();

}