package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;

public class MinTimeSerieFunction implements TimeseriesFunction {
    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.MIN;
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
                .put("datapoints",  firstEntryPoints);

        for (int i = 0; i < totalPoints; i++) {
            for (int j = 1; j < seriesCount; j++) {
                try {
                    Double newValue = timeseries.getJsonObject(j).getJsonArray("datapoints")
                            .getJsonArray(i).getDouble(0);
                    if (newValue < firstEntryPoints.getJsonArray(i).getDouble(0)) {
                        Long newTime = firstEntryPoints.getJsonArray(i).getLong(1);
                        firstEntryPoints.getJsonArray(i).clear();
                        firstEntryPoints.getJsonArray(i).add(newValue).add(newTime);
                    }
                }catch (Exception ex){
                    // do nothing here
                }

            }
        }

        return new JsonArray().add(result);
    }
}
