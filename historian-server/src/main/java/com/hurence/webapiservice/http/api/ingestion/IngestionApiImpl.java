package com.hurence.webapiservice.http.api.ingestion;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.ingestion.util.DataConverter;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.FileUpload;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.CREATED;


public class IngestionApiImpl implements IngestionApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiImpl.class);
    private HistorianService service;

    public IngestionApiImpl(HistorianService service) {
        this.service = service;

    }

    @Override
    public void importJson(RoutingContext context) {
        final ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder;
        final ImportRequestParser.ResponseAndErrorHolderAllFiles responseAndErrorHolderAllFiles = new ImportRequestParser.ResponseAndErrorHolderAllFiles();
        try {
            JsonArray getMetricsParam = context.getBodyAsJsonArray();
            responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(getMetricsParam);
            responseAndErrorHolderAllFiles.correctPoints.add(responseAndErrorHolder.correctPoints);
        }catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage("BAD REQUEST");
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(String.valueOf(errorObject));
            return;
        }

        service.rxAddTimeSeries(responseAndErrorHolderAllFiles.correctPoints)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(response -> {
                    if (responseAndErrorHolder.errorMessages.isEmpty()) {
                        context.response().setStatusCode(200);
                    } else {
                        context.response().setStatusCode(CREATED);
                    }
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(constructFinalResponseJson(response, responseAndErrorHolder).encodePrettily());
                    LOGGER.info("response : {}", response);
                }).subscribe();
    }

    private JsonObject constructFinalResponseJson(JsonObject response, ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder) {
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
    private StringBuilder extractFinalErrorMessage(ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(". ").append(responseAndErrorHolder.errorMessages.get(0));
        if (responseAndErrorHolder.errorMessages.size() > 1)
            for (int i = 1; i < responseAndErrorHolder.errorMessages.size()-1; i++) {
                errorMessage.append("\n").append(responseAndErrorHolder.errorMessages.get(i));
            }
        return errorMessage;
    }

    @Override
    public void importCsv(RoutingContext context) {
        LOGGER.trace("received request at importCsv: {}", context.request());
        Set<FileUpload> uploads = context.fileUploads();

        final ImportRequestParser.ResponseAndErrorHolderAllFiles responseAndErrorHolderAllFiles = new ImportRequestParser.ResponseAndErrorHolderAllFiles();

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
                     fileInArray = ConvertCsvFileToJson(uploadedFile, context);
                } catch (IOException e) {
                    String errorMessage = "The csv contains " + e.getMessage() + " lines which is more than the max number of line of 5000";
                    JsonObject errorObject = new JsonObject().put(FILE, currentFileUpload.fileName()).put(CAUSE, errorMessage);
                    responseAndErrorHolderAllFiles.tooBigFiles.add(errorObject);
                    continue;
                }
                responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(fileInArray);
                if (!responseAndErrorHolder.correctPoints.isEmpty())
                    responseAndErrorHolderAllFiles.correctPoints.add(responseAndErrorHolder.correctPoints);
                if (!responseAndErrorHolder.errorMessages.isEmpty())
                    responseAndErrorHolderAllFiles.errorMessages.add(responseAndErrorHolder.errorMessages);
                responseAndErrorHolderAllFiles.groupedBy = responseAndErrorHolder.groupedBy;  // here the group by and tags should be the same for every file
                responseAndErrorHolderAllFiles.tags = responseAndErrorHolder.tags;
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
        try {
            service.rxAddTimeSeries(responseAndErrorHolderAllFiles.correctPoints)
                    .doOnError(ex -> {
                        LOGGER.error("Unexpected error : ", ex);
                        context.response().setStatusCode(500);
                        context.response().putHeader("Content-Type", "application/json");
                        context.response().end(ex.getMessage());
                    })
                    .doOnSuccess(response -> {
                        if (responseAndErrorHolderAllFiles.errorMessages.isEmpty()) {
                            context.response().setStatusCode(200);
                        } else {
                            context.response().setStatusCode(CREATED);
                        }
                        context.response().putHeader("Content-Type", "application/json");
                        context.response().end(constructFinalResponseCsv(response, responseAndErrorHolderAllFiles).encodePrettily());
                        LOGGER.info("response : {}", response);
                    }).subscribe();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private JsonObject constructFinalResponseCsv(JsonObject response, ImportRequestParser.ResponseAndErrorHolderAllFiles responseAndErrorHolderAllFiles) {
        JsonArray result = response.getJsonArray(CSV);
        JsonObject finalResponse = new JsonObject();
        if (!result.isEmpty())
            finalResponse.put(TAGS, responseAndErrorHolderAllFiles.tags)
                .put(GROUPED_BY_IN_RESPONSE, responseAndErrorHolderAllFiles.groupedBy)
                .put(REPORT, result);
        if (!responseAndErrorHolderAllFiles.tooBigFiles.isEmpty())
                finalResponse.put(ERRORS, responseAndErrorHolderAllFiles.tooBigFiles);
        return finalResponse;
    }

    private JsonArray ConvertCsvFileToJson (File file, RoutingContext context) throws IOException {
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map> rows = csvMapper
                .readerWithSchemaFor(Map.class)
                .with(CsvSchema.emptySchema().withHeader())
                .readValues(file);
        DataConverter converter = new DataConverter(context);
        List<Map<String, Object>> result = converter.toGroupedByMetricDataPoints(rows);
        if (result.size()-1 > MAX_LINGE_FOR_CSV_FILE) {
            throw new IOException(String.valueOf(result.size()-1));
        }
        return new JsonArray(result);

    }
}
