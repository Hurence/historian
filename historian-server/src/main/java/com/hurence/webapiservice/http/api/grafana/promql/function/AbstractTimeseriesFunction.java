package com.hurence.webapiservice.http.api.grafana.promql.function;

import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;

public abstract class AbstractTimeseriesFunction implements TimeseriesFunction {

    protected QueryRequest request;

    public TimeseriesFunction setQueryRequest(QueryRequest request) {
        this.request = request;
        return this;
    }

    public QueryRequest getRequest() {
        return request;
    }
}
