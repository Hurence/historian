package com.hurence.webapiservice.http.api.grafana.promql.function;

import com.hurence.forecasting.Forecaster;
import com.hurence.forecasting.ForecasterFactory;
import com.hurence.forecasting.ForecastingAlgorithm;
import com.hurence.timeseries.model.Measure;
import io.vertx.core.json.JsonArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PredictArimaTimeSerieFunction extends AbstractTimeseriesFunction {


    public static final int NUM_FORECASTED_POINTS = 20;
    private Forecaster<Double> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.ARIMA_DOUBLE);

    private boolean isTrained = false;

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
        for (int j = 0; j < seriesCount; j++) {
            List<Double> values = new ArrayList<>();
            Long latestTime = 0L;
            Long timeStep = 0L;
            for (int i = 0; i < totalPoints; i++) {

                JsonArray dataPoints = timeseries.getJsonObject(j).getJsonArray("datapoints").getJsonArray(i);

                values.add(dataPoints.getDouble(0));
                Long newTime = dataPoints.getLong(1);
                if (latestTime != newTime) {
                    timeStep = newTime - latestTime;
                    latestTime = newTime;
                }
            }
            try {
              //  if(!isTrained)
                {
                    forecaster.fit(values.subList(0, values.size()-10), values.subList(10, values.size()));
                    isTrained =true;
                }
                List<Double> forecasted = forecaster.forecast(values, NUM_FORECASTED_POINTS);
                timeseries.getJsonObject(j).getJsonArray("datapoints").clear();
                timeseries.getJsonObject(j).put("total_points", NUM_FORECASTED_POINTS);
                for (Double value : forecasted) {
                    Long newTime = latestTime + timeStep;
                    latestTime = newTime;
                    timeseries.getJsonObject(j).getJsonArray("datapoints")
                            .add(new JsonArray().add(value).add(newTime));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



        return timeseries;
    }
}
