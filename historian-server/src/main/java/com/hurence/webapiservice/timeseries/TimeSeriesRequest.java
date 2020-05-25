package com.hurence.webapiservice.timeseries;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;

import static com.hurence.historian.modele.HistorianFields.*;

public interface TimeSeriesRequest {
    long getFrom();

    List<AGG> getAggs();

    long getTo();

    SamplingConf getSamplingConf();

    List<String> getMetricNames();

    Map<String, String> getTags();

    String getRequestId();
}
