package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.FileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class IngestionApiUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiUtil.class);

    public static ImportRequestParser.CorrectPointsAndFailedPointsOfAllFiles fromUploadsToCorrectPointsAndFailedPointsOfAllFiles(Set<FileUpload> uploads, MultiMap multiMap) {
        ImportRequestParser.CorrectPointsAndFailedPointsOfAllFiles correctPointsAndFailedPointsOfAllFiles = new ImportRequestParser.CorrectPointsAndFailedPointsOfAllFiles();
        for (FileUpload currentFileUpload : uploads) {
            LOGGER.info("uploaded currentFileUpload : {} of size : {}", currentFileUpload.fileName(), currentFileUpload.size());
            LOGGER.info("contentType currentFileUpload : {} of contentTransferEncoding : {}", currentFileUpload.contentType(), currentFileUpload.contentTransferEncoding());
            LOGGER.info("uploaded uploadedFileName : {} ", currentFileUpload.uploadedFileName());
            LOGGER.info("uploaded charSet : {} ", currentFileUpload.charSet());
            String fileName = currentFileUpload.uploadedFileName();
            File uploadedFile = new File(fileName);

            final ImportRequestParser.CorrectPointsAndFailedPoints correctPointsAndFailedPoints;
            JsonArray fileInArray;
            try {
                fileInArray = ConvertCsvFileToJson(uploadedFile, multiMap);
            } catch (IOException e) {
                String errorMessage = "The csv contains " + e.getMessage() + " lines which is more than the max number of line of "+MAX_LINES_FOR_CSV_FILE;
                JsonObject errorObject = new JsonObject().put(FILE, currentFileUpload.fileName()).put(CAUSE, errorMessage);
                correctPointsAndFailedPointsOfAllFiles.namesOfTooBigFiles.add(errorObject);
                continue;
            }

            correctPointsAndFailedPoints = new ImportRequestParser().parseCsvImportRequest(fileInArray, multiMap);

            if (!correctPointsAndFailedPoints.correctPoints.isEmpty())
                correctPointsAndFailedPointsOfAllFiles.correctPoints.addAll(correctPointsAndFailedPoints.correctPoints);
            if (!correctPointsAndFailedPoints.numberOfFailedPointsPerMetric.isEmpty())
                correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric.mergeIn(correctPointsAndFailedPoints.numberOfFailedPointsPerMetric);
        }
        return correctPointsAndFailedPointsOfAllFiles;
    }

    /**
     * @param file        the csv File to be converted to json
     *
     * @param multiMap    the MultiMap that comes with the file
     *
     * @return result      a json array containing json Objects, each one has the points grouped by the name, the date and the tags if there is any to
     *                      group by with
     *                                       [
     *                                        {
     *                                            "name": ... ,
     *                                            "tags": {
     *                                              "sensor": "sensor_1",
     *                                              "code_install": "code_1"
     *                                              }
     *                                            "points": [
     *                                               [time, value],
     *                                               [time, value]
     *                                            ]
     *                                        },
     *                                       {
     *                                            "name": ... ,
     *                                            "tags": {
     *                                                    "sensor": "sensor_2",
     *                                                    "code_install": "code_2"
     *                                                    }
     *                                            "points": [
     *                                                [time, value],
     *                                                [time, value]
     *                                            ]
     *                                       }
     *                                       ]
     */

    public static JsonArray ConvertCsvFileToJson(File file, MultiMap multiMap) throws IOException {
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map> rows = csvMapper
                .readerWithSchemaFor(Map.class)
                .with(CsvSchema.emptySchema().withHeader())
                .readValues(file);

        List<LineWithDateInfo> linesWithDateInfo = addDateInfoToEachLine(rows, multiMap);

        DataConverter converter = new DataConverter(multiMap);
        JsonArray result = converter.toGroupedByMetricDataPoints(linesWithDateInfo);
        if (result.size() > MAX_LINES_FOR_CSV_FILE) {
            throw new IOException(String.valueOf(result.size()));
        }
        return result;
    }

    private static List<LineWithDateInfo> addDateInfoToEachLine(MappingIterator<Map> rows, MultiMap multiMap) throws IOException {
        List<Map> listFromRows = rows.readAll();
        List<LineWithDateInfo> linesWithDateInfos = new LinkedList<>();
        listFromRows.forEach(i -> {
            try {
                String date = generateDateFromTime(i, multiMap);
                linesWithDateInfos.add(new LineWithDateInfo(i,date));
            }catch (Exception e) {
                LOGGER.debug("error in parsing date", e);
            }
        });
        return linesWithDateInfos;
    }
    private static String generateDateFromTime (Map map, MultiMap multiMap) {
        if (multiMap.get(MAPPING_TIMESTAMP) == null)
            multiMap.add(MAPPING_TIMESTAMP, "timestamp");
        Object date1 = map.get(multiMap.get(MAPPING_TIMESTAMP));
        long date = (long) DataConverter.toNumber(date1, multiMap);
        Date d = new Date(date);
        DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        return f.format(d);
    }


    public static JsonObject constructFinalResponseCsv(ImportRequestParser.CorrectPointsAndFailedPointsOfAllFiles correctPointsAndFailedPointsOfAllFiles, io.vertx.reactivex.core.MultiMap multiMap) {

        JsonArray result = getGroupedByReportFromInjectedPoints(correctPointsAndFailedPointsOfAllFiles, multiMap);
        JsonObject finalResponse = new JsonObject();
        if (!result.isEmpty())
            finalResponse.put(TAGS, new JsonArray(multiMap.getAll(MAPPING_TAGS)))
                    .put(GROUPED_BY_IN_RESPONSE, IngestionApiUtil.getGroupedByList(multiMap))
                    .put(REPORT, result);
        if (!correctPointsAndFailedPointsOfAllFiles.namesOfTooBigFiles.isEmpty())
            finalResponse.put(ERRORS, correctPointsAndFailedPointsOfAllFiles.namesOfTooBigFiles);
        return finalResponse;
    }
    public static JsonArray getGroupedByReportFromInjectedPoints(ImportRequestParser.CorrectPointsAndFailedPointsOfAllFiles correctPointsAndFailedPointsOfAllFiles, io.vertx.reactivex.core.MultiMap multiMap) {

        final JsonObject failedPoints = correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric;
        List<Map<String, Object>> resultForCsvImport = new LinkedList<>();
        try {
            JsonArray timeseriesPoints = correctPointsAndFailedPointsOfAllFiles.correctPoints;
            JsonArray groupedByFields = IngestionApiUtil.getGroupedByList(multiMap);
            List<HashMap<String, String>> groupedByFieldsForEveryChunk = new LinkedList<>();
            for (Object timeserieObject : timeseriesPoints) {
                JsonObject timeserie = (JsonObject) timeserieObject;
                JsonObject tagsObject = timeserie.getJsonObject(TAGS);
                HashMap<String, String> groupedByFieldsForThisChunk = new LinkedHashMap<String,String>();
                groupedByFields.forEach(f -> {
                    if (f.toString().equals(NAME))
                        groupedByFieldsForThisChunk.put(f.toString(),timeserie.getString(f.toString()));
                    else
                        groupedByFieldsForThisChunk.put(f.toString(),tagsObject.getString(f.toString()));
                });
                int totalNumPointsInChunk = timeserie.getJsonArray(POINTS_REQUEST_FIELD).size();
                groupedByFieldsForThisChunk.put("totalPointsForThisChunk", String.valueOf(totalNumPointsInChunk));
                groupedByFieldsForEveryChunk.add(groupedByFieldsForThisChunk);
            }
            resultForCsvImport.addAll(prepareOneReport(groupedByFieldsForEveryChunk, groupedByFields, failedPoints));
        } catch (Exception e) {
            LOGGER.error("unexpected exception");
        }
        return new JsonArray(sortTheCsvIngestionResult(resultForCsvImport));
    }
    static List<Map<String, Object>> prepareOneReport(List<HashMap<String, String>> groupedByFieldsForEveryChunk, JsonArray groupBdByFields, JsonObject failedPoints) {
        return groupedByFieldsForEveryChunk.stream()
                .collect(Collectors.groupingBy(map -> {
                            HashMap<String, String> groupedByFieldsForThisMap = new LinkedHashMap<String,String>();
                            groupBdByFields.forEach(f -> groupedByFieldsForThisMap.put(f.toString(), map.get(f)));
                            return groupedByFieldsForThisMap;
                        },LinkedHashMap::new,
                        Collectors.mapping(map -> map.get("totalPointsForThisChunk"),
                                Collectors.toList()))).entrySet().stream().map(entry -> {
                    Map<String, Object> resultObject = new LinkedHashMap<>();
                    JsonArray groupByListForThisChunk = new JsonArray();
                    int totalNumberOfPointsPerGroupedFildes = 0;
                    int chunkNumber = entry.getValue().size();
                    entry.getKey().forEach(resultObject::put);
                    totalNumberOfPointsPerGroupedFildes = entry.getValue().stream().mapToInt(Integer::valueOf).sum();
                    resultObject.put("number_of_points_injected", totalNumberOfPointsPerGroupedFildes);
                    entry.getKey().entrySet().forEach(entry1 -> {
                        groupByListForThisChunk.add(entry1.getValue());
                    });
                    resultObject.put("number_of_point_failed", failedPoints.getInteger(groupByListForThisChunk.toString(), 0));
                    resultObject.put("number_of_chunk_created", chunkNumber);
                    return resultObject;
                }).collect(Collectors.toList());
    }

    public static JsonObject constructFinalResponseJson(JsonObject response, ImportRequestParser.CorrectPointsAndErrorMessages responseAndErrorHolder) {
        StringBuilder message = new StringBuilder();
        message.append("Injected ").append(response.getInteger(RESPONSE_TOTAL_ADDED_POINTS)).append(" points of ")
                .append(response.getInteger(RESPONSE_TOTAL_ADDED_CHUNKS))
                .append(" metrics in ").append(response.getInteger(RESPONSE_TOTAL_ADDED_CHUNKS))
                .append(" chunks");
        JsonObject finalResponse = new JsonObject();
        if (!responseAndErrorHolder.errorMessages.isEmpty()) {
            message.append(extractFinalErrorMessage(responseAndErrorHolder).toString());
            finalResponse.put("status", "Done but got some errors").put("message", message.toString());
        }else
            finalResponse.put("status", "OK").put("message", message.toString());
        return finalResponse;
    }
    public static StringBuilder extractFinalErrorMessage(ImportRequestParser.CorrectPointsAndErrorMessages responseAndErrorHolder) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(". ").append(responseAndErrorHolder.errorMessages.get(0));
        if (responseAndErrorHolder.errorMessages.size() > 1)
            for (int i = 1; i < responseAndErrorHolder.errorMessages.size()-1; i++) {
                errorMessage.append("\n").append(responseAndErrorHolder.errorMessages.get(i));
            }
        return errorMessage;
    }

    public static List<Map<String, Object>> sortTheCsvIngestionResult (List<Map<String, Object>> result) {
        result.sort(new Comparator<Map<String, Object>>() {

            @Override
            public int compare(Map<String, Object> a, Map<String, Object> b) {
                String valA = "";
                String valB = "";

                valA = (String) a.get(NAME);
                valB = (String) b.get(NAME);

                return valA.compareTo(valB);
            }
        });
        return new ArrayList<>(result);
    }

    public static JsonArray getGroupedByList(MultiMap multiMap) {
        List<String> groupByList = multiMap.getAll(GROUP_BY).stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(NAME))
                return s;
            else return null ;
        }).collect(Collectors.toList());
        groupByList.remove(null);
        return new JsonArray(groupByList);
    }

    public static class LineWithDateInfo {
        public Map mapFromOneCsvLine;
        public String date;

        LineWithDateInfo(Map map, String date) {
            this.mapFromOneCsvLine = map;
            this.date = date;
        }
    }

}
