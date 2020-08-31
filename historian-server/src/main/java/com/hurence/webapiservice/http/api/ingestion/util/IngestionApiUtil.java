package com.hurence.webapiservice.http.api.ingestion.util;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class IngestionApiUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiUtil.class);

    public static void constructCsvFileConvertors(MultiCsvFilesConvertor multiCsvFilesConvertor) {
        multiCsvFilesConvertor.uploads.forEach(file -> multiCsvFilesConvertor.csvFileConvertors.add(new CsvFileConvertor(multiCsvFilesConvertor.multiMap, file)));
        multiCsvFilesConvertor.parseFiles();
        multiCsvFilesConvertor.fillingAllFilesConvertor();
    }

    public static JsonObject constructFinalResponseCsv(MultiCsvFilesConvertor.CorrectPointsAndFailedPointsOfAllFiles correctPointsAndFailedPointsOfAllFiles,
                                                       io.vertx.reactivex.core.MultiMap multiMap) {

        JsonArray result = getGroupedByReportFromInjectedPoints(correctPointsAndFailedPointsOfAllFiles, multiMap);
        JsonObject finalResponse = new JsonObject();
        if (!result.isEmpty())
            finalResponse.put(HistorianServiceFields.TAGS, new JsonArray(multiMap.getAll(HistorianServiceFields.MAPPING_TAGS)))
                    .put(HistorianServiceFields.GROUPED_BY_IN_RESPONSE, IngestionApiUtil.getGroupedByList(multiMap))
                    .put(HistorianServiceFields.REPORT, result);
        if (!correctPointsAndFailedPointsOfAllFiles.namesOfTooBigFiles.isEmpty())
            finalResponse.put(HistorianServiceFields.ERRORS, correctPointsAndFailedPointsOfAllFiles.namesOfTooBigFiles);
        return finalResponse;
    }
    public static JsonArray getGroupedByReportFromInjectedPoints(MultiCsvFilesConvertor.CorrectPointsAndFailedPointsOfAllFiles correctPointsAndFailedPointsOfAllFiles,
                                                                 io.vertx.reactivex.core.MultiMap multiMap) {

        List<Map<String, Object>> resultForCsvImport = new LinkedList<>();
        try {
            JsonArray timeseriesPoints = correctPointsAndFailedPointsOfAllFiles.correctPoints;
            JsonArray groupedByFields = IngestionApiUtil.getGroupedByList(multiMap);
            List<HashMap<String, String>> groupedByFieldsForEveryChunk = new LinkedList<>();
            for (Object timeserieObject : timeseriesPoints) {
                JsonObject timeserie = (JsonObject) timeserieObject;
                JsonObject tagsObject = timeserie.getJsonObject(HistorianServiceFields.TAGS);
                HashMap<String, String> groupedByFieldsForThisChunk = new LinkedHashMap<String,String>();
                groupedByFields.forEach(f -> {
                    if (f.toString().equals(HistorianServiceFields.NAME))
                        groupedByFieldsForThisChunk.put(f.toString(),timeserie.getString(f.toString()));
                    else
                        groupedByFieldsForThisChunk.put(f.toString(),tagsObject.getString(f.toString()));
                });
                int totalNumPointsInChunk = timeserie.getJsonArray(HistorianServiceFields.POINTS).size();
                groupedByFieldsForThisChunk.put("totalPointsForThisChunk", String.valueOf(totalNumPointsInChunk));
                groupedByFieldsForEveryChunk.add(groupedByFieldsForThisChunk);
            }
            resultForCsvImport.addAll(prepareOneReport(groupedByFieldsForEveryChunk, groupedByFields, correctPointsAndFailedPointsOfAllFiles.numberOfFailedPointsPerMetric));
        } catch (Exception e) {
            LOGGER.error("unexpected exception");
        }
        return new JsonArray(sortTheCsvIngestionResult(resultForCsvImport));
    }
    static List<Map<String, Object>> prepareOneReport(List<HashMap<String, String>> groupedByFieldsForEveryChunk, JsonArray groupBdByFields, LinkedHashMap failedPoints) {
        List<Map<String, Object>> listOfReports = groupedByFieldsForEveryChunk.stream()
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
                    entry.getKey().forEach((key, value) -> groupByListForThisChunk.add(value));
                    int integer = (int) failedPoints.get(entry.getKey());
                    resultObject.put("number_of_point_failed", integer);
                    failedPoints.remove(entry.getKey());
                    resultObject.put("number_of_chunk_created", chunkNumber);
                    return resultObject;
                }).collect(Collectors.toList());
        failedPoints.forEach((i,j) -> {
            Map<String, Object> resultObject = new LinkedHashMap<>();
            LinkedHashMap<String, String> fields = (LinkedHashMap<String, String>) i;
            fields.forEach((y,z) -> resultObject.put(y, z));
            resultObject.put("number_of_point_failed", j);
            listOfReports.add(resultObject);
        });
        return listOfReports;
    }

    public static JsonObject constructFinalResponseJson(JsonObject response, ImportRequestParser.CorrectPointsAndErrorMessages responseAndErrorHolder) {
        StringBuilder message = new StringBuilder();
        message.append("Injected ").append(response.getInteger(HistorianServiceFields.TOTAL_ADDED_POINTS)).append(" points of ")
                .append(response.getInteger(HistorianServiceFields.TOTAL_ADDED_CHUNKS))
                .append(" metrics in ").append(response.getInteger(HistorianServiceFields.TOTAL_ADDED_CHUNKS))
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

                valA = (String) a.get(HistorianServiceFields.NAME);
                valB = (String) b.get(HistorianServiceFields.NAME);

                return valA.compareTo(valB);
            }
        });
        return new ArrayList<>(result);
    }

    public static JsonArray getGroupedByList(MultiMap multiMap) {
        List<String> groupByList = multiMap.getAll(HistorianServiceFields.GROUP_BY).stream().map(s -> {
            if (s.startsWith(HistorianServiceFields.TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(HistorianServiceFields.NAME))
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
