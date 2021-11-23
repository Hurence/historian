package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;

public class SumTimeSerieFunction implements TimeseriesFunction {

    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.SUM;
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
            Double valueSum = firstEntryPoints.getJsonArray(i).getDouble(0);
            Long time = firstEntryPoints.getJsonArray(i).getLong(1);
            for (int j = 1; j < seriesCount; j++) {
                try{
                valueSum += timeseries.getJsonObject(j).getJsonArray("datapoints")
                        .getJsonArray(i).getDouble(0);
                }catch (Exception ex){
                    // do nothing
                }
            }
            firstEntryPoints.getJsonArray(i).clear();
            firstEntryPoints.getJsonArray(i).add(valueSum).add(time);
        }

        return new JsonArray().add(result);
    }
}
