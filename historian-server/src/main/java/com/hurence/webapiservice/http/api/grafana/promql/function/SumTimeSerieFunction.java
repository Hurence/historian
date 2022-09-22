package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

public class SumTimeSerieFunction implements TimeseriesFunction {

    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.SUM;
    }


    @Override
    public JsonArray process(JsonArray timeseries) {

        if (timeseries.size() < 1)
            return timeseries;

        // first compute an array of all the timestamps of all series
        TreeSet<Long> allTimestamps = new TreeSet<>();
        for (int i = 0; i < timeseries.size(); i++) {
            JsonObject currentEntry = timeseries.getJsonObject(i);
            JsonArray currentEntryPoints = currentEntry.getJsonArray("datapoints");
            for (int j = 0; j < currentEntryPoints.size(); j++) {
                allTimestamps.add(currentEntryPoints.getJsonArray(j).getLong(1));
            }
        }

        // this will give te total number of points we will aggregate
        int totalPoints = allTimestamps.size();
        JsonArray aggregatedValues = new JsonArray();

        JsonObject result = new JsonObject()
                .put("name", timeseries.getJsonObject(0).getString("name"))
                .put("tags", new HashMap<>())
                .put("total_points", totalPoints)
                .put("datapoints", aggregatedValues);

        int i=0;
        for (Long currentTimestamp : allTimestamps) {
            double valueSum = 0.0;
            Long time = 0L;
            JsonArray dataPoints = new JsonArray();

            for (int j = 0; j < timeseries.size(); j++) {
                try {
                    final JsonArray currentDataPoints = timeseries.getJsonObject(j).getJsonArray("datapoints");
                    for (int k = 0; k < currentDataPoints.size(); k++) {
                        if (currentDataPoints.getJsonArray(k).getLong(1).equals(currentTimestamp)){
                            final Double currentValue = currentDataPoints.getJsonArray(k).getDouble(0);
                            if(!Double.isNaN(currentValue)) {
                                valueSum += currentValue;
                                break;
                            }
                        }
                    }

                } catch (Exception ex) {
                    // do nothing
                }
            }
            dataPoints.add(valueSum).add(currentTimestamp);
            aggregatedValues.add(dataPoints);
            i++;
        }

        return new JsonArray().add(result);
    }

}
