package com.hurence.webapiservice.http.api.modele;

import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;

import java.util.List;
import java.util.Map;

public interface TimeSeriesRequest {
    long getFrom();

    List<AGG> getAggs();

    long getTo();

    SamplingConf getSamplingConf();

    List<String> getMetricNames();

    Map<String, String> getTags();

    String getRequestId();
}
