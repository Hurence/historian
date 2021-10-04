package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;

/**
 * This function does nothing on the timeseries
 */
public class NoopTimeSerieFunction implements TimeseriesFunction {
    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.NOOP;
    }

    @Override
    public JsonArray process(JsonArray timeseries) {
        return timeseries;
    }
}
