package com.hurence.webapiservice.historian.models;

import com.hurence.historian.model.HistorianServiceFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.timeseries.model.Definitions.FIELD_NAME;

public class ResponseAsList {

    public List<SubResponse> responseList;

    public ResponseAsList(JsonArray timeseries) {
        List<SubResponse> jsonArray = new ArrayList<>();
        for (Object timeserie : timeseries) {
            JsonObject json = (JsonObject) timeserie;
            JsonArray datapoints = json.getJsonArray(HistorianServiceFields.DATAPOINTS);
            for (Object datapoint : datapoints) {
                JsonArray line = new JsonArray();
                line.add(json.getString(FIELD_NAME));
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
        public String quality = "NaN";

        public SubResponse(JsonArray line) {
            try{
                this.quality = line.getFloat(3).toString();
                this.metric = line.getString(0);
                this.date = line.getLong(2);
                this.value = line.getDouble(1);
            }catch (Exception ex) {
                this.metric = line.getString(0);
                this.date = line.getLong(2);
                this.value = line.getDouble(1);
            }
        }
    }

    public List<SubResponse> ReturnList() {
        return this.responseList;
    }
}
