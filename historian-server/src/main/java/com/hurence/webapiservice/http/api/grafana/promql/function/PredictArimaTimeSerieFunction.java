package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;

public class PredictArimaTimeSerieFunction implements TimeseriesFunction {

    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.PREDICT_ARIMA;
    }

    @Override
    public JsonArray process(JsonArray timeseries) {

        int seriesCount = timeseries.size();
        if (seriesCount < 1)
            return timeseries;

        int totalPoints = timeseries.getJsonObject(0).getJsonArray("datapoints").size();

        for (int i = 0; i < totalPoints; i++) {
            for (int j = 0; j < seriesCount; j++) {
                JsonArray dataPoints = timeseries.getJsonObject(j).getJsonArray("datapoints").getJsonArray(i);
                Double newValue = Math.abs(dataPoints.getDouble(0));

                Long newTime = dataPoints.getLong(1);
                dataPoints.clear()
                        .add(newValue)
                        .add(newTime);
            }
        }

        return timeseries;
    }
}
