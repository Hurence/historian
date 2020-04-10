package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.http.grafana.modele.AnnotationRequestType;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;

import java.util.List;

public interface AnnotationRequest {
    Long getFrom();

    Long getTo();

    AnnotationRequestType getType();

    List<String> getTags();

    int getMaxAnnotation();

    boolean getMatchAny();

}
