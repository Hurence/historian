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
    /**
     * @param jsonImportRequest        JsonArray that have the jsonObjects that will be added, ex :
     *                                 [
     *                                  {
     *                                      "name": ...
     *                                      "points": [
     *                                          [time, value],
     *                                          [time, value]
     *                                      ]
     *                                  },
     *                                 {
     *                                      "name": ...
     *                                      "points": [
     *                                          [time, value],
     *                                          [time, value]
     *                                      ]
     *                                 }
     *                                 ]
     *
     * @return CorrectPointsAndErrorMessages an object that have the correct points in a jsonArray and a list of the error
     *          messages if there is bad points
     */
    public static CorrectPointsAndErrorMessages parseJsonImportRequest(JsonArray jsonImportRequest) {
        if (null == jsonImportRequest) {
            throw new NullPointerException("Null request body");
        }else if (jsonImportRequest.isEmpty()) {
            throw new IllegalArgumentException("Empty request body");
        }
        CorrectPointsAndErrorMessages correctPointsAndErrorMessages = new CorrectPointsAndErrorMessages();
        for (Object metricsObject : jsonImportRequest) {
            JsonObject timeserie = (JsonObject) metricsObject;
            if (!(timeserie.containsKey(NAME)))
                throw new IllegalArgumentException("Missing a name for at least one metric");
            if (((timeserie.getValue(NAME) == null) && (timeserie.getValue(POINTS_REQUEST_FIELD) != null)) || (timeserie.getValue(NAME) == "") ) {
                int numPoints = timeserie.getJsonArray(POINTS_REQUEST_FIELD).size();
                correctPointsAndErrorMessages.errorMessages.add("Ignored "+ numPoints +" points for metric with name "+timeserie.getValue(NAME)+" because this is not a valid name");
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
                String commonErrorMessage = "Ignored 1 points for metric with name '" + timeserie.getString(NAME);
                try {
                    pointArray = (JsonArray) point;
                    pointArray.size();
                } catch (Exception ex) {
                    correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because it was not an array");
                    continue;
                }
                if (pointArray.size() == 0){
                    correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because this point was an empty array");
                    continue;
                } else if (pointArray.size() != 2)
                    throw new IllegalArgumentException("Points should be of the form [timestamp, value]");
                try {
                    if (pointArray.getLong(0) == null) {
                        correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its timestamp is null");
                        continue;
                    }
                } catch (Exception e) {
                    correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its timestamp is not a long");
                    continue;
                }
                try {
                    if ((pointArray.getDouble(1) == null)) {
                        correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its value was not a double");
                        continue;
                    }
                } catch (Exception e) {
                    correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its value was not a double");
                    continue;
                }
                newPoints.add(pointArray);
            }
            if(!newPoints.isEmpty()) {
                newTimeserie.put(POINTS_REQUEST_FIELD, newPoints);
                correctPointsAndErrorMessages.correctPoints.add(newTimeserie);
            }
        }
        if (correctPointsAndErrorMessages.correctPoints.isEmpty())
            throw new IllegalArgumentException("There is no valid points");
        return correctPointsAndErrorMessages;
    }
    public static class CorrectPointsAndErrorMessages {

        public JsonArray correctPoints;
        public ArrayList<String> errorMessages;

        CorrectPointsAndErrorMessages () {
            this.correctPoints = new JsonArray();
            this.errorMessages = new ArrayList<>();
        }
    }
    public CorrectPointsAndFailedPoints parseCsvImportRequest(JsonArray metricsParam, MultiMap multiMap) throws IllegalArgumentException {
        if (null == metricsParam) {
            throw new NullPointerException("Null request body");
        }else if (metricsParam.isEmpty()) {
            throw new IllegalArgumentException("Empty request body");
        }
        CorrectPointsAndFailedPoints correctPointsAndFailedPoints = new CorrectPointsAndFailedPoints();
        for (Object metricsObject : metricsParam) {
            JsonObject timeserie = (JsonObject) metricsObject;
            int numberOfFailedPointsForThisName = 0;
            if (!(timeserie.containsKey(NAME)))
                throw new IllegalArgumentException("Missing a name for at least one metric");
            if (((timeserie.getValue(NAME) == null) && (timeserie.getValue(POINTS_REQUEST_FIELD) != null)) || (timeserie.getValue(NAME) == "") ) {
                int numPoints = timeserie.getJsonArray(POINTS_REQUEST_FIELD).size();
                numberOfFailedPointsForThisName = numberOfFailedPointsForThisName + numPoints;
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
                try {
                    pointArray = (JsonArray) point;
                    pointArray.size();
                } catch (Exception ex) {
                    numberOfFailedPointsForThisName++;
                    continue;
                }
                if (pointArray.size() == 0){
                    numberOfFailedPointsForThisName++;
                    continue;
                } else if (pointArray.size() != 2)
                    throw new IllegalArgumentException("Points should be of the form [timestamp, value]");
                try {
                    if (pointArray.getLong(0) == null) {
                        numberOfFailedPointsForThisName++;
                        continue;
                    }
                } catch (Exception e) {
                    numberOfFailedPointsForThisName++;
                    continue;
                }
                try {
                    if ((pointArray.getDouble(1) == null)) {
                        numberOfFailedPointsForThisName++;
                        continue;
                    }
                } catch (Exception e) {
                    numberOfFailedPointsForThisName++;
                    continue;
                }
                newPoints.add(pointArray);
            }
            if(!newPoints.isEmpty()) {
                newTimeserie.put(POINTS_REQUEST_FIELD, newPoints);
                correctPointsAndFailedPoints.correctPoints.add(newTimeserie);
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
            if (correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.containsKey(groupByArray.toString())) {
                currentNumberOfFailedPoints = correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.getInteger(groupByArray.toString());
                correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.put(groupByArray.toString(), currentNumberOfFailedPoints+numberOfFailedPointsForThisName);
            }else {
                correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.put(groupByArray.toString(), numberOfFailedPointsForThisName);
            }

        }
        if (correctPointsAndFailedPoints.correctPoints.isEmpty())
            throw new IllegalArgumentException("There is no valid points");
        return correctPointsAndFailedPoints;
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
    public static class CorrectPointsAndFailedPointsOfAllFiles {

        public JsonArray correctPoints;
        public JsonArray namesOfTooBigFiles;
        public JsonObject numberOfFailedPointsPerMetric;

        public CorrectPointsAndFailedPointsOfAllFiles() {
            this.correctPoints = new JsonArray();
            this.namesOfTooBigFiles = new JsonArray();
            this.numberOfFailedPointsPerMetric = new JsonObject();
        }
    }
    public static class CorrectPointsAndFailedPoints {

        public JsonArray correctPoints;
        public JsonObject numberOfFailedPointsPerMetric;

        CorrectPointsAndFailedPoints () {
            this.correctPoints = new JsonArray();
            this.numberOfFailedPointsPerMetric = new JsonObject();
        }
    }

}
