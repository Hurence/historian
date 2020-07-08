package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.webapiservice.http.api.ingestion.util.CsvFileConvertor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class ImportRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportRequestParser.class);

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
                } else if (pointArray.size() != 2 && pointArray.size() != 3)
                    throw new IllegalArgumentException("Points should be of the form [timestamp, value] or [timestamp, value, quality]");
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
                if (pointArray.size() == 3) {
                    try {
                        if (pointArray.getFloat(2) == null) {
                            correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its quality is null");
                            continue;
                        } else if ((pointArray.getFloat(2) > 1) || (pointArray.getFloat(2) < 0)) {
                            correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its quality is not between 0 and 1");
                            continue;
                        }
                    } catch (Exception e) {
                        correctPointsAndErrorMessages.errorMessages.add(commonErrorMessage + "' because its quality is not a float");
                        continue;
                    }
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
    public CsvFileConvertor.CorrectPointsAndFailedPoints parseCsvImportRequest(JsonArray metricsParam, MultiMap multiMap) throws IllegalArgumentException {
        if (null == metricsParam) {
            throw new NullPointerException("Null request body");
        }else if (metricsParam.isEmpty()) {
            throw new IllegalArgumentException("Empty request body");
        }
        CsvFileConvertor.CorrectPointsAndFailedPoints correctPointsAndFailedPoints = new CsvFileConvertor.CorrectPointsAndFailedPoints();
        for (Object metricsObject : metricsParam) {
            JsonObject timeserie = (JsonObject) metricsObject;
            int numberOfFailedPointsForThisName = 0;
            if (!(timeserie.containsKey(NAME)))
                throw new IllegalArgumentException("Missing a name for at least one metric");
            if (((timeserie.getValue(NAME) == null) && (timeserie.getValue(POINTS_REQUEST_FIELD) != null)) || (timeserie.getValue(NAME) == "") ) {
                int numPoints = timeserie.getJsonArray(POINTS_REQUEST_FIELD).size();
                numberOfFailedPointsForThisName = numberOfFailedPointsForThisName + numPoints; // TODO see this
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
                } else if (pointArray.size() != 3 && pointArray.size() != 2)
                    throw new IllegalArgumentException("Points should be of the form [timestamp, value] or [timestamp, value, quality]");
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
                if (pointArray.size() == 3) {
                    try {
                        if ((pointArray.getFloat(2) == null) || (pointArray.getFloat(2) > 1.0) || (pointArray.getFloat(2) < 0.0)) {
                            numberOfFailedPointsForThisName++;
                            continue;
                        }
                    } catch (Exception e) {
                        numberOfFailedPointsForThisName++;
                        continue;
                    }
                }
                newPoints.add(pointArray);
            }
            if(!newPoints.isEmpty()) {
                newTimeserie.put(POINTS_REQUEST_FIELD, newPoints);
                correctPointsAndFailedPoints.correctPoints.add(newTimeserie);
            }
            int currentNumberOfFailedPoints;
            LinkedHashMap groupByMap = new LinkedHashMap();
            if (multiMap != null) {
                List<String> groupByList = multiMap.getAll(GROUP_BY).stream().map(s -> {
                    if (s.startsWith(TAGS+".")) {
                        groupByMap.put(s.substring(5), timeserie.getJsonObject(TAGS).getString(s.substring(5)));
                        return timeserie.getJsonObject(TAGS).getString(s.substring(5));
                    }else if (s.equals(NAME)) {
                        groupByMap.put(s, timeserie.getString(s));
                        return timeserie.getString(s);
                    } else return null ;
                }).collect(Collectors.toList());
                groupByList.remove(null);
            } else {
            groupByMap.put(NAME, timeserie.getString(NAME));
            }
            if (correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.containsKey(groupByMap)) {
                currentNumberOfFailedPoints = (int) correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.get(groupByMap);
                correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.put(groupByMap, currentNumberOfFailedPoints+numberOfFailedPointsForThisName);
            }else {
                correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.put(groupByMap, numberOfFailedPointsForThisName);
            }

        }
        if (correctPointsAndFailedPoints.correctPoints.isEmpty())
            throw new IllegalArgumentException("There is no valid points");
        return correctPointsAndFailedPoints;
    }
}
