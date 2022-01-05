package com.hurence.webapiservice.http.api.grafana.promql.function;

import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;

public class TimeserieFunctionFactory {

    public static TimeseriesFunction getInstance(QueryRequest request) {

        TimeseriesFunction function = new NoopTimeSerieFunction();
        if (request.getQuery().getAggregationOperator().isPresent()) {
            switch (request.getQuery().getAggregationOperator().get()) {
                case MIN:
                    function = new MinTimeSerieFunction();
                    break;
                case MAX:
                    function = new MaxTimeSerieFunction();
                    break;
                case ABS:
                    function = new AbsTimeSerieFunction();
                    break;
                case AVG:
                    function = new AvgTimeSerieFunction();
                    break;
                case SUM:
                    function = new SumTimeSerieFunction();
                    break;
                case PREDICT_ARIMA:
                    function = new PredictArimaTimeSerieFunction();
                    break;
                default:
                    function = new NoopTimeSerieFunction();
            }
        }
        return function.setQueryRequest(request);


    }

}
