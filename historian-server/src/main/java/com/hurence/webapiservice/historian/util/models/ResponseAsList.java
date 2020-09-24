package com.hurence.webapiservice.historian.util.models;

import com.hurence.historian.modele.HistorianFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class ResponseAsList {

    public List<SubResponse> responseList;

    public ResponseAsList(JsonArray timeseries) {
        List<SubResponse> jsonArray = new ArrayList<>();
        for (Object timeserie : timeseries) {
            JsonObject json = (JsonObject) timeserie;
            JsonArray datapoints = json.getJsonArray(HistorianFields.DATAPOINTS);
            for (Object datapoint : datapoints) {
                JsonArray line = new JsonArray();
                line.add(json.getString(HistorianFields.NAME));
                for (Object point : (JsonArray) datapoint) {
                    line.add(point);
                }
                SubResponse subResponse = new SubResponse(line);
                jsonArray.add(subResponse);
            }
        }
        this.responseList = jsonArray;
    }

    public static class SubResponse {
        public String metric;
        public Long date;
        public double value;

        public SubResponse(JsonArray line) {
            this.metric = line.getString(0);
            this.date = line.getLong(2);
            this.value = line.getDouble(1);
        }
    }

    public List<SubResponse> ReturnList() {
        return this.responseList;
    }
}