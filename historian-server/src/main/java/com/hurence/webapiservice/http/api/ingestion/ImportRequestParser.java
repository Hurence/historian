package com.hurence.webapiservice.http.api.ingestion;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class ImportRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportRequestParser.class);


    public ResponseAndErrorHolder checkAndBuildValidHistorianImportRequest(JsonArray metricsParam, MultiMap multiMap) throws IllegalArgumentException {
        if (null == metricsParam) {
            throw new NullPointerException("Null request body");
        }else if (metricsParam.isEmpty()) {
            throw new IllegalArgumentException("Empty request body");
        }
        ResponseAndErrorHolder responseAndErrorHolder = new ResponseAndErrorHolder();
        for (Object metricsObject : metricsParam) {
            JsonObject timeserie = (JsonObject) metricsObject;
            int numberOfFailedPointsForThisName = 0;
            if (!(timeserie.containsKey(NAME)))
                throw new IllegalArgumentException("Missing a name for at least one metric");
            if (((timeserie.getValue(NAME) == null) && (timeserie.getValue(POINTS_REQUEST_FIELD) != null)) || (timeserie.getValue(NAME) == "") ) {
                int numPoints = timeserie.getJsonArray(POINTS_REQUEST_FIELD).size();
                responseAndErrorHolder.errorMessages.add("Ignored "+ numPoints +" points for metric with name "+timeserie.getValue(NAME)+" because this is not a valid name");
                numberOfFailedPointsForThisName = numberOfFailedPointsForThisName + numPoints; // fix this
                continue;
            } else if (!(timeserie.getValue(NAME) instanceof String)) {
                throw new IllegalArgumentException("A name is not a string for at least one metric");
            } else if (!(timeserie.containsKey(POINTS_REQUEST_FIELD))) {
                throw new IllegalArgumentException("field 'points' is required");
            } else if  ((!(timeserie.getValue(POINTS_REQUEST_FIELD) instanceof JsonArray)) || (timeserie.getValue(POINTS_REQUEST_FIELD)==null)) {
                throw new IllegalArgumentException("field 'points' : " + timeserie.getValue(POINTS_REQUEST_FIELD) + " is not an array");
            }
            JsonObject newTimeserie = new JsonObject();
            Set<String> fieldsNamesWithoutPoints = timeserie.fieldNames();
            fieldsNamesWithoutPoints.forEach(i -> {
                    if (!i.equals(POINTS_REQUEST_FIELD))
                        newTimeserie.put(i, timeserie.getValue(i));
            });
            JsonArray newPoints = new JsonArray();
            for (Object point : timeserie.getJsonArray(POINTS_REQUEST_FIELD)) {
                JsonArray pointArray;
                String commonErrorMessage = "Ignored 1 points for metric with name '" + timeserie.getString(NAME);
                try {
                    pointArray = (JsonArray) point;
                    pointArray.size();
                } catch (Exception ex) {
                    responseAndErrorHolder.errorMessages.add(commonErrorMessage + "' because it was not an array");
                    numberOfFailedPointsForThisName++;
                    continue;
                }
                if (pointArray.size() == 0){
                    responseAndErrorHolder.errorMessages.add(commonErrorMessage + "' because this point was an empty array");
                    numberOfFailedPointsForThisName++;
                    continue;
                } else if (pointArray.size() != 2)
                    throw new IllegalArgumentException("Points should be of the form [timestamp, value]");
                try {
                    if (pointArray.getLong(0) == null) {
                        responseAndErrorHolder.errorMessages.add(commonErrorMessage + "' because its timestamp is null");
                        numberOfFailedPointsForThisName++;
                        continue;
                    }
                } catch (Exception e) {
                    responseAndErrorHolder.errorMessages.add(commonErrorMessage + "' because its timestamp is not a long");
                    numberOfFailedPointsForThisName++;
                    continue;
                }
                try {
                    if ((pointArray.getDouble(1) == null)) {
                        responseAndErrorHolder.errorMessages.add(commonErrorMessage + "' because its value was not a double");
                        numberOfFailedPointsForThisName++;
                        continue;
                    }
                } catch (Exception e) {
                    responseAndErrorHolder.errorMessages.add(commonErrorMessage + "' because its value was not a double");
                    numberOfFailedPointsForThisName++;
                    continue;
                }
                newPoints.add(pointArray);
            }
            if(!newPoints.isEmpty()) {
                newTimeserie.put(POINTS_REQUEST_FIELD, newPoints);
                responseAndErrorHolder.correctPoints.add(newTimeserie);
            }
            int currentNumberOfFailedPoints;
            JsonArray groupByArray = new JsonArray();
            if (multiMap != null) {
                List<String> groupByList = multiMap.getAll(GROUP_BY).stream().map(s -> {
                    if (s.startsWith(TAGS+".")) {
                        return s.substring(5);
                    }else if (s.equals(NAME))
                        return s;
                    else return null ;
                }).collect(Collectors.toList());
                groupByList.remove(null);
                groupByList.forEach(i -> groupByArray.add(timeserie.getValue(i)));
            } else {
                groupByArray.add(NAME);
            }
            if (responseAndErrorHolder.numberOfFailedPointsPerMetric.containsKey(groupByArray.toString())) {
                currentNumberOfFailedPoints = responseAndErrorHolder.numberOfFailedPointsPerMetric.getInteger(groupByArray.toString());
                responseAndErrorHolder.numberOfFailedPointsPerMetric.put(groupByArray.toString(), currentNumberOfFailedPoints+numberOfFailedPointsForThisName);
            }else {
                responseAndErrorHolder.numberOfFailedPointsPerMetric.put(groupByArray.toString(), numberOfFailedPointsForThisName);
            }

        }
        if (responseAndErrorHolder.correctPoints.isEmpty())
            throw new IllegalArgumentException("There is no valid points");
        return responseAndErrorHolder;
    }

    public static class ResponseAndErrorHolder {

        public JsonArray correctPoints;
        public ArrayList<String> errorMessages;
        public JsonObject numberOfFailedPointsPerMetric;

        ResponseAndErrorHolder () {
            this.correctPoints = new JsonArray();
            this.errorMessages = new ArrayList<>();
            this.numberOfFailedPointsPerMetric = new JsonObject();
        }
    }
    public static class ResponseAndErrorHolderAllFiles {

        public JsonObject correctPoints;
        public ArrayList<ArrayList<String>> errorMessages;
        public JsonArray tooBigFiles;
        public JsonObject numberOfFailedPointsPerMetric;

        ResponseAndErrorHolderAllFiles () {
            this.correctPoints = new JsonObject();
            correctPoints.put(CORRECT_POINTS, new JsonArray());
            this.errorMessages = new ArrayList<>();
            this.tooBigFiles = new JsonArray();
            this.numberOfFailedPointsPerMetric = new JsonObject();
        }
    }
}
