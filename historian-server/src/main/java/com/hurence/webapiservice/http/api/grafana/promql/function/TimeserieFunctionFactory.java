package com.hurence.webapiservice.http.api.grafana.promql.function;

public class TimeserieFunctionFactory {

    public static TimeseriesFunction getInstance( TimeserieFunctionType type){
        switch (type){
            case MIN: return new MinTimeSerieFunction();
            case MAX: return new MaxTimeSerieFunction();
            case ABS: return new AbsTimeSerieFunction();
            case AVG: return new AvgTimeSerieFunction();
            case SUM: return new SumTimeSerieFunction();
            case COUNT: return new CountTimeSerieFunction();
            case PREDICT_ARIMA: return new PredictArimaTimeSerieFunction();
            default: return new NoopTimeSerieFunction();
        }
    }

}
