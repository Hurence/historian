package com.hurence.webapiservice.http.api.grafana.promql.function;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

public class SumTimeSerieFunction extends AbstractTimeseriesFunction {


    @Override
    public TimeserieFunctionType type() {
        return TimeserieFunctionType.SUM;
    }

    public JsonObject processAtomic(JsonArray timeseries) {

        int seriesCount = timeseries.size();

        SortedMap<Long, List<Double>> valuesMap = new TreeMap<>();
        Map<String, Object> tags = new HashMap<>();
        String name ="";
        for (int j = 0; j < seriesCount; j++) {
            name = timeseries.getJsonObject(j).getString("name");
            JsonArray datapoints = timeseries.getJsonObject(j).getJsonArray("datapoints");
            tags = timeseries.getJsonObject(j).getJsonObject("tags").getMap();


            for (int i = 0; i < datapoints.size(); i++) {
                Long timestamp = datapoints.getJsonArray(i).getLong(1);
                Double value = datapoints.getJsonArray(i).getDouble(0);

                // initialize list if needed
                if (!valuesMap.containsKey(timestamp)) {
                    valuesMap.put(timestamp, new ArrayList<>());
                }
                valuesMap.get(timestamp).add(value);
            }
        }

        Integer totalPoints = valuesMap.size();
        JsonObject result = new JsonObject()
                .put("name", "sum(" + name + ")")
                .put("tags", tags)
                .put("total_points", totalPoints)
                .put("datapoints", new JsonArray());

        for (Long timetamp : valuesMap.keySet()) {
            Double sum = valuesMap.get(timetamp).stream().reduce(0.0, Double::sum);

            result.getJsonArray("datapoints")
                    .add(new JsonArray().add(sum).add(timetamp));
        }

        return result;

    }

    @Override
    public JsonArray process(JsonArray timeseries) {

        int seriesCount = timeseries.size();
        if (seriesCount < 1)
            return timeseries;

        JsonArray result = new JsonArray();

        // do we need to sum by ?
        if(!request.getQuery().getGroupByParameter().getNames().isEmpty()){
            List<String> names = request.getQuery().getGroupByParameter().getNames();

            // will build a map of series points grouped by tags
            Map<String, JsonArray> groupedArrays = new HashMap<>();
            for (int j = 0; j < seriesCount; j++) {
                JsonObject currentObject = timeseries.getJsonObject(j);
                Map<String, Object> tags = currentObject.getJsonObject("tags").getMap();

                Map<String, String> groupedTags = new HashMap<>();
                String keyName = "";
                for(String key : tags.keySet()){
                    if (names.contains(key)){
                        String value = String.valueOf(tags.get(key));
                        groupedTags.put(key, value);

                        keyName += String.format("%s-%s!",key, value) ;
                    }
                }

                // replace tags
                currentObject.put("tags", groupedTags);

                // init if new key
                if(!groupedArrays.containsKey(keyName)){
                    groupedArrays.put(keyName, new JsonArray());
                }

                groupedArrays.get(keyName).add(currentObject);
            }

            // will process each group of series
            for(String key : groupedArrays.keySet()){
                result.add(processAtomic(groupedArrays.get(key)));
            }

        }else{
            // single sum remove all tags
            JsonObject entries = processAtomic(timeseries);
            entries.put("tags", new HashMap<>());
            result.add(entries);
        }

        return result;
    }
}
