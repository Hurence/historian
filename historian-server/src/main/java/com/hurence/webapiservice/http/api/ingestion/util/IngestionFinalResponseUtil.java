package com.hurence.webapiservice.http.api.ingestion.util;

import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;


public class IngestionFinalResponseUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionFinalResponseUtil.class);

    /**
     * @param allFilesReport          AllFilesReport
     * @param csvFilesConvertorConf   CsvFilesConvertorConf
     *
     * construct the final response to return, which should be grouped by.
     *
     * @return JsonObject : the response to return
     */
    public static JsonObject constructFinalResponseCsv(AllFilesReport allFilesReport,
                                                       CsvFilesConvertorConf csvFilesConvertorConf) {

        JsonArray report = getGroupedByReportFromInjectedPoints(allFilesReport, csvFilesConvertorConf);

        JsonObject finalResponse = new JsonObject();

        if (!report.isEmpty())
            finalResponse.put(FIELD_TAGS, new JsonArray(csvFilesConvertorConf.getTags()))
                    .put(GROUPED_BY_IN_RESPONSE, new JsonArray(csvFilesConvertorConf.getGroupByListWithNAME()))
                    .put(REPORT, report);

        if (!allFilesReport.filesThatFailedToBeImported.isEmpty())
            finalResponse.put(ERRORS, allFilesReport.filesThatFailedToBeImported);

        return finalResponse;
    }

    /**
     * @param allFilesReport          AllFilesReport
     * @param csvFilesConvertorConf   CsvFilesConvertorConf
     *
     * get the grouped by report.
     *
     * @return JsonArray : the report.
     */
    public static JsonArray getGroupedByReportFromInjectedPoints(AllFilesReport allFilesReport,
                                                                 CsvFilesConvertorConf csvFilesConvertorConf) {

        List<Map<String, Object>> resultForCsvImport = new LinkedList<>();
        try {

            JsonArray timeseriesPoints = allFilesReport.correctPoints;
            JsonArray groupedByFields = new JsonArray(csvFilesConvertorConf.getGroupByListWithNAME());
            List<HashMap<String, String>> groupedByFieldsForEveryChunk = new LinkedList<>();

            fillingThe_groupedByFieldsForEveryChunk_List(timeseriesPoints, groupedByFields, groupedByFieldsForEveryChunk);

            List<Map<String, Object>> listOfReports = constructTheFinalReport(groupedByFieldsForEveryChunk, groupedByFields, allFilesReport.numberOfFailedPointsPerMetric);

            resultForCsvImport.addAll(listOfReports);

        } catch (Exception e) {
            LOGGER.error("unexpected exception");
        }

        return new JsonArray(sortTheFinalReport(resultForCsvImport));
    }

    /**
     * @param timeseriesPoints               JsonArray
     * @param groupedByFields                JsonArray
     * @param groupedByFieldsForEveryChunk   List<HashMap<String, String>>
     *
     * fill the groupedByFieldsForEveryChunk list which contains the fields to group by for every chunk,
     *                                       and add the element totalPointsForThisChunk for every chunk
     *
     * @return void
     */
    private static void fillingThe_groupedByFieldsForEveryChunk_List(JsonArray timeseriesPoints, JsonArray groupedByFields,
                                                                     List<HashMap<String, String>> groupedByFieldsForEveryChunk ) {
        for (Object timeserieObject : timeseriesPoints) {
            JsonObject timeserie = (JsonObject) timeserieObject;
            JsonObject tagsObject = timeserie.getJsonObject(FIELD_TAGS);
            HashMap<String, String> groupedByFieldsForThisChunk = new LinkedHashMap<>();
            groupedByFields.forEach(f -> {
                if (f.toString().equals(FIELD_NAME))
                    groupedByFieldsForThisChunk.put(f.toString(),timeserie.getString(f.toString()));
                else
                    groupedByFieldsForThisChunk.put(f.toString(),tagsObject.getString(f.toString()));
            });
            int totalNumPointsInChunk = timeserie.getJsonArray(POINTS).size();
            groupedByFieldsForThisChunk.put("totalPointsForThisChunk", String.valueOf(totalNumPointsInChunk));
            groupedByFieldsForEveryChunk.add(groupedByFieldsForThisChunk);
        }
    }

    /**
     * @param groupedByFieldsForEveryChunk   List<HashMap<String, String>>
     * @param groupedByFields                JsonArray
     * @param failedPoints   LinkedHashMap<LinkedHashMap<String,String>, Integer>
     *
     * construct the final report from the grouped by Map from methode getCustomGroupedByMap : looks like :
     *                       "report" : [
     *                                      {
     *                                          "name": "metric_1",
     *                                          "number_of_points_injected": 3,
     *                                          "number_of_point_failed": 0,
     *                                          "number_of_chunk_created": 2
 *                                          }
     *                                  ]
     *
     *                      ...
     *
     * @return List<Map<String, Object>>
     */
    private static List<Map<String, Object>> constructTheFinalReport(List<HashMap<String, String>> groupedByFieldsForEveryChunk,
                                                                     JsonArray groupedByFields,
                                                                     LinkedHashMap<LinkedHashMap<String,String>, Integer> failedPoints) {
        LinkedHashMap<HashMap<String, String>, List<String>> customGroupedByMap = getCustomGroupedByMap(groupedByFieldsForEveryChunk, groupedByFields);
        List<Map<String, Object>> finalReport = customGroupedByMap
                .entrySet().stream().map(entry -> {
                    Map<String, Object> resultObject = new LinkedHashMap<>();
                    int totalNumberOfPointsPerGroupedFiles = 0;
                    int chunkNumber = entry.getValue().size();
                    entry.getKey().forEach(resultObject::put);
                    totalNumberOfPointsPerGroupedFiles = entry.getValue().stream().mapToInt(Integer::valueOf).sum();
                    resultObject.put("number_of_points_injected", totalNumberOfPointsPerGroupedFiles);
                    int integer = failedPoints.get(entry.getKey());
                    resultObject.put("number_of_point_failed", integer);
                    failedPoints.remove(entry.getKey());
                    resultObject.put("number_of_chunk_created", chunkNumber);
                    return resultObject;
                }).collect(Collectors.toList());
        failedPoints.forEach((i,j) -> {
            Map<String, Object> resultObject = new LinkedHashMap<>();
            i.forEach(resultObject::put);
            resultObject.put("number_of_point_failed", j);
            finalReport.add(resultObject);
        });

        return finalReport;
    }

    /**
     * @param groupedByFieldsForEveryChunk       List<HashMap<String, String>>
     * @param groupedByFields                    JsonArray
     *
     * get a grouped by Map from groupedByFieldsForEveryChunk,
     *                                           group by the map (name_of_the_field, value_of_the_field) where "field" is every element in groupedByFields
     *                                           and collect the element totalPointsForThisChunk.
     *
     * @return LinkedHashMap<HashMap<String, String>, List<String>>
     */
    private static LinkedHashMap<HashMap<String, String>, List<String>> getCustomGroupedByMap(List<HashMap<String, String>> groupedByFieldsForEveryChunk, JsonArray groupedByFields) {
        return groupedByFieldsForEveryChunk.stream()
                .collect(Collectors.groupingBy(map -> {
                            HashMap<String, String> groupedByFieldsForThisMap = new LinkedHashMap<>();
                            groupedByFields.forEach(f -> groupedByFieldsForThisMap.put(f.toString(), map.get(f)));
                            return groupedByFieldsForThisMap;
                        },
                        LinkedHashMap::new,
                        Collectors.mapping(map -> map.get("totalPointsForThisChunk"), Collectors.toList()))
                );
    }

    /**
     * @param result       List<Map<String, Object>>
     *
     * sort the finalReport .
     *
     * @return List<Map<String, Object>>
     */
    public static List<Map<String, Object>> sortTheFinalReport(List<Map<String, Object>> result) {
        result.sort((a, b) -> {
            String valA = "";
            String valB = "";

            valA = (String) a.get(FIELD_NAME);
            valB = (String) b.get(FIELD_NAME);

            return valA.compareTo(valB);
        });
        return new ArrayList<>(result);
    }

    public static JsonObject constructFinalResponseJson(JsonObject response, ImportRequestParser.CorrectPointsAndErrorMessages responseAndErrorHolder) {
        StringBuilder message = new StringBuilder();
        message.append("Injected ").append(response.getInteger(TOTAL_ADDED_POINTS)).append(" points of ")
                .append(response.getInteger(TOTAL_ADDED_CHUNKS))
                .append(" metrics in ").append(response.getInteger(TOTAL_ADDED_CHUNKS))
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

}
