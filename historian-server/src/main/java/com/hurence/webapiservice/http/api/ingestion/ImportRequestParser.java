package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.webapiservice.http.api.ingestion.util.CsvFilesConvertorConf;
import com.hurence.webapiservice.http.api.ingestion.util.FileReport;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;

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
    /**
     * @param fileInArray              JsonArray
     * @param csvFilesConvertorConf    CsvFilesConvertorConf
     * @param fileReport               FileReport
     *
     * parse the json and fill the fileReport:
     *                      - fill the correctPoints
     *                      - fill the numberOfFailedPointsPerMetric
     *
     * @return void
     */
    public static FileReport parseCsvImportRequest(JsonArray fileInArray, CsvFilesConvertorConf csvFilesConvertorConf, FileReport fileReport) {
        if (null == fileInArray) {
            throw new NullPointerException("Null request body");
        }else if (fileInArray.isEmpty()) {
            throw new IllegalArgumentException("Empty request body");
        }
        parseEachObjectInTheArray(fileInArray, fileReport, csvFilesConvertorConf);
        if (fileReport.correctPoints.isEmpty())
            throw new IllegalArgumentException("There is no valid points");
        return fileReport;
    }

    /**
     * @param fileInArray              JsonArray
     * @param fileReport               FileReport
     * @param csvFilesConvertorConf    CsvFilesConvertorConf
     *
     * parse every object in fileInArray
     *
     * @return void
     */
    private static void parseEachObjectInTheArray(JsonArray fileInArray, FileReport fileReport, CsvFilesConvertorConf csvFilesConvertorConf) {
        for (Object timeserieObject : fileInArray) {
            JsonObject timeserie = (JsonObject) timeserieObject;
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
            JsonObject newTimeserie = getTimeserieWithoutPoints(timeserie);
            parseEachPointInTheObject(timeserie, numberOfFailedPointsForThisName, fileReport, newTimeserie);
            calculateNumberOfFailedPoints(fileReport, csvFilesConvertorConf, timeserie, numberOfFailedPointsForThisName);

        }
    }

    /**
     * @param fileReport                         FileReport
     * @param csvFilesConvertorConf              CsvFilesConvertorConf
     * @param timeserie                          JsonObject
     * @param numberOfFailedPointsForThisName    int
     *
     * calculate the number of failed points per metric
     *
     * @return void
     */
    private static void calculateNumberOfFailedPoints(FileReport fileReport, CsvFilesConvertorConf csvFilesConvertorConf, JsonObject timeserie, int numberOfFailedPointsForThisName) {
        int currentNumberOfFailedPoints;
        LinkedHashMap groupByMap = getGroupedByFields(csvFilesConvertorConf, timeserie);
        if (fileReport.numberOfFailedPointsPerMetric.containsKey(groupByMap)) {
            currentNumberOfFailedPoints = (int) fileReport.numberOfFailedPointsPerMetric.get(groupByMap);
            fileReport.numberOfFailedPointsPerMetric.put(groupByMap, currentNumberOfFailedPoints+numberOfFailedPointsForThisName);
        }else {
            fileReport.numberOfFailedPointsPerMetric.put(groupByMap, numberOfFailedPointsForThisName);
        }
    }

    private static JsonObject getTimeserieWithoutPoints (JsonObject timeserie) {
        //TODO
        return null;
    }

    private static LinkedHashMap getGroupedByFields (CsvFilesConvertorConf csvFilesConvertorConf, JsonObject timeserie) {
        //TODO
        return null;
    }

    /**
     * @param timeserie                         JsonObject
     * @param numberOfFailedPointsForThisName   int
     * @param fileReport                        FileReport
     * @param newTimeserie                      JsonObject
     *
     * parse every point in the object
     *
     * @return void
     */
    private static void parseEachPointInTheObject(JsonObject timeserie, int numberOfFailedPointsForThisName, FileReport fileReport, JsonObject newTimeserie) {
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
            fileReport.correctPoints.add(newTimeserie);
        }
    }
}
