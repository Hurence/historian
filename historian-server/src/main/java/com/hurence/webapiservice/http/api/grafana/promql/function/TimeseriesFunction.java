package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;

/**
 * modify timeseries accordlingly to a function (such as abs(), avg())
 */
public interface TimeseriesFunction {

    TimeserieFunctionType type();
    JsonArray process(JsonArray timeseries);
}
