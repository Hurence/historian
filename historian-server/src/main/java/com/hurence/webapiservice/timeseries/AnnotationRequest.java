package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestType;

import java.util.List;

public interface AnnotationRequest {
    Long getFrom();

    Long getTo();

    AnnotationRequestType getType();

    List<String> getTags();

    int getMaxAnnotation();

    boolean getMatchAny();

}
