package com.hurence.webapiservice.http.api.modele;

import com.hurence.webapiservice.http.api.grafana.model.AnnotationRequestType;

import java.util.List;

public interface AnnotationRequest {
    Long getFrom();

    Long getTo();

    AnnotationRequestType getType();

    List<String> getTags();

    int getMaxAnnotation();

    boolean getMatchAny();

}
