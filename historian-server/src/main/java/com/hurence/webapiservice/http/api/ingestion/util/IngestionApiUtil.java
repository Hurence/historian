package com.hurence.webapiservice.http.api.ingestion.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.webapiservice.http.api.ingestion.ImportRequestParser;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.FileUpload;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;

public class IngestionApiUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiUtil.class);

    public static void fromUploadsFilesToResponseAndErrorHolderAllFiles(Set<FileUpload> uploads,
                                                                        ImportRequestParser.ResponseAndErrorHolderAllFiles responseAndErrorHolderAllFiles,
                                                                        RoutingContext context,
                                                                        MultiMap multiMap) {
        for (FileUpload currentFileUpload : uploads) {
            LOGGER.info("uploaded currentFileUpload : {} of size : {}", currentFileUpload.fileName(), currentFileUpload.size());
            LOGGER.info("contentType currentFileUpload : {} of contentTransferEncoding : {}", currentFileUpload.contentType(), currentFileUpload.contentTransferEncoding());
            LOGGER.info("uploaded uploadedFileName : {} ", currentFileUpload.uploadedFileName());
            LOGGER.info("uploaded charSet : {} ", currentFileUpload.charSet());
            String fileName = currentFileUpload.uploadedFileName();
            File uploadedFile = new File(fileName);

            final ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder;
            try {
                JsonArray fileInArray;
                try {
                    fileInArray = ConvertCsvFileToJson(uploadedFile, multiMap);
                } catch (IOException e) {
                    String errorMessage = "The csv contains " + e.getMessage() + " lines which is more than the max number of line of "+MAX_LINES_FOR_CSV_FILE;
                    JsonObject errorObject = new JsonObject().put(FILE, currentFileUpload.fileName()).put(CAUSE, errorMessage);
                    responseAndErrorHolderAllFiles.tooBigFiles.add(errorObject);
                    continue;
                }
                responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(fileInArray, multiMap);
                if (!responseAndErrorHolder.correctPoints.isEmpty())
                    responseAndErrorHolderAllFiles.correctPoints.getJsonArray(CORRECT_POINTS).add(responseAndErrorHolder.correctPoints);
                if (!responseAndErrorHolder.errorMessages.isEmpty())
                    responseAndErrorHolderAllFiles.errorMessages.add(responseAndErrorHolder.errorMessages);
                if (!responseAndErrorHolder.numberOfFailedPointsPerMetric.isEmpty())
                    responseAndErrorHolderAllFiles.numberOfFailedPointsPerMetric.mergeIn(responseAndErrorHolder.numberOfFailedPointsPerMetric);
            } catch (Exception ex) {
                JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
                LOGGER.error("error parsing request", ex);
                context.response().setStatusCode(BAD_REQUEST);
                context.response().setStatusMessage("BAD REQUEST");
                context.response().putHeader("Content-Type", "application/json");
                context.response().end(String.valueOf(errorObject));
                return;
            }
        }
        List<String> groupByList = multiMap.getAll(GROUP_BY).stream().map(s -> {
            if (s.startsWith(TAGS+".")) {
                return s.substring(5);
            }else if (s.equals(NAME))
                return s;
            else return null ;
        }).collect(Collectors.toList());
        groupByList.remove(null);
        responseAndErrorHolderAllFiles.correctPoints.put(GROUPED_BY, new JsonArray(groupByList))
                .put(TAGS, new JsonArray(multiMap.getAll(MAPPING_TAGS)))
                .put("number of failed points", responseAndErrorHolderAllFiles.numberOfFailedPointsPerMetric)
                .put(IMPORT_TYPE, "ingestion-csv");
    }

    public static JsonArray ConvertCsvFileToJson(File file, MultiMap multiMap) throws IOException {
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map> rows = csvMapper
                .readerWithSchemaFor(Map.class)
                .with(CsvSchema.emptySchema().withHeader())
                .readValues(file);
        List<Map> list = rows.readAll();
        list.forEach(i -> {
            try {
                if (multiMap.get(MAPPING_TIMESTAMP) == null)
                    multiMap.add(MAPPING_TIMESTAMP, "timestamp");
                Object date1 = i.get(multiMap.get(MAPPING_TIMESTAMP));
                long date = (long) DataConverter.toNumber(date1, multiMap);
                Date d = new Date(date); // here see what is the default timestamp format
                DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
                i.put(DATE, f.format(d));
            }catch (Exception e) {
                LOGGER.debug("error in parsing date", e);
            }
        });
        DataConverter converter = new DataConverter(multiMap);
        JsonArray result = converter.toGroupedByMetricDataPoints(list);
        if (result.size() > MAX_LINES_FOR_CSV_FILE) {
            throw new IOException(String.valueOf(result.size()));
        }
        return result;
    }

    public static JsonObject constructFinalResponseCsv(JsonObject response, ImportRequestParser.ResponseAndErrorHolderAllFiles responseAndErrorHolderAllFiles) {
        JsonArray result = response.getJsonArray(CSV);
        JsonObject finalResponse = new JsonObject();
        if (!result.isEmpty())
            finalResponse.put(TAGS, responseAndErrorHolderAllFiles.correctPoints.getJsonArray(TAGS))
                    .put(GROUPED_BY_IN_RESPONSE, responseAndErrorHolderAllFiles.correctPoints.getJsonArray(GROUPED_BY))
                    .put(REPORT, result);
        if (!responseAndErrorHolderAllFiles.tooBigFiles.isEmpty())
            finalResponse.put(ERRORS, responseAndErrorHolderAllFiles.tooBigFiles);
        return finalResponse;
    }

    public static JsonObject constructFinalResponseJson(JsonObject response, ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder) {
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
    public static StringBuilder extractFinalErrorMessage(ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(". ").append(responseAndErrorHolder.errorMessages.get(0));
        if (responseAndErrorHolder.errorMessages.size() > 1)
            for (int i = 1; i < responseAndErrorHolder.errorMessages.size()-1; i++) {
                errorMessage.append("\n").append(responseAndErrorHolder.errorMessages.get(i));
            }
        return errorMessage;
    }
}
