package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;

import java.util.List;

public interface AnnotationRequest {
    long getFrom();

    long getTo();

    long getFromRaw();

    long getToRaw();

    SamplingConf getSamplingConf();

    String getType();

    List<String> getMetricNames();

    List<String> getTags();

    JsonArray getTag();

    int getMaxAnnotation();

    Boolean getMatchAny();

}
