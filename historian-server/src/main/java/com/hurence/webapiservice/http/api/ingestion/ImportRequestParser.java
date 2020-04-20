package com.hurence.webapiservice.http.api.ingestion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.hurence.historian.modele.HistorianFields.*;

public class ImportRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportRequestParser.class);


    public ResponseAndErrorHolder checkAndBuildValidHistorianImportRequest(JsonArray metricsParam) throws IllegalArgumentException {
        if (null == metricsParam) {
            throw new NullPointerException("Null request body");
        }else if (metricsParam.isEmpty()) {
            throw new IllegalArgumentException("Empty request body");
        }
        ResponseAndErrorHolder responseAndErrorHolder = new ResponseAndErrorHolder();
        for (Object object : metricsParam) {
            JsonObject timeserie = (JsonObject) object;
            if (!(timeserie.containsKey(NAME))) {
                throw new IllegalArgumentException("Missing a name for at least one metric");
            } else if ((timeserie.getValue(NAME) == null) && (timeserie.getValue(POINTS_REQUEST_FIELD) != null)) {
                int numPoints = timeserie.getJsonArray(POINTS_REQUEST_FIELD).size();
                responseAndErrorHolder.errorMessages.add("Ignored "+ numPoints +" points for metric with name 'null' because this is not a valid name");
                continue;
            } else if (!(timeserie.getValue(NAME) instanceof String)) {
                throw new IllegalArgumentException("A name is not a string for at least one metric");
            } else if (!(timeserie.containsKey(POINTS_REQUEST_FIELD))) {
                throw new IllegalArgumentException("field 'points' is required");
            } else if  ((!(timeserie.getValue(POINTS_REQUEST_FIELD) instanceof JsonArray)) || (timeserie.getValue(POINTS_REQUEST_FIELD)==null)) {
                throw new IllegalArgumentException("field 'points' : " + timeserie.getValue(POINTS_REQUEST_FIELD) + " is not an array");
            }
            JsonObject newTimeserie = new JsonObject();
            newTimeserie.put(NAME, timeserie.getString(NAME));
            JsonArray newPoints = new JsonArray();
            for (Object point : timeserie.getJsonArray(POINTS_REQUEST_FIELD)) {
                JsonArray pointArray;
                String communErrorMessage = "Ignored 1 points for metric with name '" + timeserie.getString(NAME);
                try {
                    pointArray = (JsonArray) point;
                    pointArray.size();
                } catch (Exception ex) {
                    responseAndErrorHolder.errorMessages.add(communErrorMessage + "' because it was not an array");
                    continue;
                }
                if (pointArray.size() == 0){
                    responseAndErrorHolder.errorMessages.add(communErrorMessage + "' because this point was an empty array");
                    continue;
                } else if (pointArray.size() != 2)
                    throw new IllegalArgumentException("Points should be of the form [timestamp, value]");
                try {
                    if (pointArray.getLong(0) == null) {
                        responseAndErrorHolder.errorMessages.add(communErrorMessage + "' because its timestamp is null");
                        continue;
                    }
                } catch (Exception e) {
                    responseAndErrorHolder.errorMessages.add(communErrorMessage + "' because its timestamp is not a long");
                    continue;
                }
                try {
                    if ((pointArray.getDouble(1) == null)) {
                        responseAndErrorHolder.errorMessages.add(communErrorMessage + "' because its value was not a double");
                        continue;
                    }
                } catch (Exception e) {
                    responseAndErrorHolder.errorMessages.add(communErrorMessage + "' because its value was not a double");
                    continue;
                }
                newPoints.add(pointArray);
            }
            if(!newPoints.isEmpty()) {
                newTimeserie.put(POINTS_REQUEST_FIELD, newPoints);
                responseAndErrorHolder.correctPoints.add(newTimeserie);
            }
        }
        if (responseAndErrorHolder.correctPoints.isEmpty())
            throw new IllegalArgumentException("There is no valid points");
        return responseAndErrorHolder;

    }

    public static class ResponseAndErrorHolder {

        public JsonArray correctPoints;
        public ArrayList<String> errorMessages;

        ResponseAndErrorHolder () {
            this.correctPoints = new JsonArray();
            this.errorMessages = new ArrayList<>();
        }
    }
}
