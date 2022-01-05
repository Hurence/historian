package com.hurence.webapiservice.http.api.grafana.promql.function;

import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;
import io.vertx.core.json.JsonArray;

/**
 * modify timeseries accordlingly to a function (such as abs(), avg())
 */
public interface TimeseriesFunction {

    TimeseriesFunction setQueryRequest(QueryRequest request);
    TimeserieFunctionType type();
    JsonArray process(JsonArray timeseries);
}
