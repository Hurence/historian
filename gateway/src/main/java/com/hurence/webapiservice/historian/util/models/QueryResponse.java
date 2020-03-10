package com.hurence.webapiservice.historian.util.models;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class QueryResponse {

    public List<SubResponse> jsonArray;

    public QueryResponse(JsonArray timeseries) {
        List<SubResponse> jsonArray = new ArrayList<>();
        for (Object timeserie : timeseries) {
            JsonObject json = (JsonObject) timeserie;
            JsonArray datapoints = json.getJsonArray("datapoints");
            for (Object datapoint : datapoints) {
                JsonArray line = new JsonArray();
                line.add(json.getString("target"));
                for (Object point : (JsonArray) datapoint) {
                    line.add(point);
                }
                SubResponse subResponse = new SubResponse(line);
                jsonArray.add(subResponse);
            }
        }
        this.jsonArray = jsonArray;
    }

    public static class SubResponse {
        public String metric;
        public Long date;
        public Long value;

        public SubResponse(JsonArray line) {
            this.metric = line.getString(0);
            this.date = line.getLong(1);
            this.value = line.getLong(2);
        }
    }

    public List<SubResponse> ReturnList() {
        return this.jsonArray;
    }
}
