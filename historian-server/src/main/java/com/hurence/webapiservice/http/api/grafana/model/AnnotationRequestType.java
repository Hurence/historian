package com.hurence.webapiservice.http.api.grafana.model;

import java.util.Arrays;

public enum AnnotationRequestType {
    ALL,
    TAGS;

    public static String getValuesAsString() {
        return Arrays.toString(values());
    }
}
