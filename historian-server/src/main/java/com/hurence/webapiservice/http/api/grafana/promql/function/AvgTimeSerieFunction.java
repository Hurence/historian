package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;

public class AvgTimeSerieFunction extends AbstractTimeseriesFunction {

    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.AVG;
    }

    @Override
    public JsonArray process(JsonArray timeseries) {

        int seriesCount = timeseries.size();
        if (seriesCount < 1)
            return timeseries;

        JsonObject firstEntry = timeseries.getJsonObject(0);
        JsonArray firstEntryPoints = firstEntry.getJsonArray("datapoints");

        Integer totalPoints = firstEntry.getInteger("total_points");
        JsonObject result = new JsonObject()
                .put("name", firstEntry.getString("name"))
                .put("tags", new HashMap<>())
                .put("total_points", totalPoints)
                .put("datapoints", firstEntryPoints);

        for (int i = 0; i < totalPoints; i++) {
            try {
                Double valueSum = firstEntryPoints.getJsonArray(i).getDouble(0);
                Long time = firstEntryPoints.getJsonArray(i).getLong(1);
                for (int j = 1; j < seriesCount; j++) {
                    valueSum += timeseries.getJsonObject(j).getJsonArray("datapoints")
                            .getJsonArray(i).getDouble(0);
                }
                firstEntryPoints.getJsonArray(i).clear();
                firstEntryPoints.getJsonArray(i).add(valueSum / seriesCount).add(time);
            } catch (Exception ex) {
                // do nothing
            }

        }

        return new JsonArray().add(result);
    }
}
