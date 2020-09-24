package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.reactivex.HistorianService;

import com.hurence.webapiservice.http.api.ingestion.util.CsvFilesConvertorConf;
import com.hurence.webapiservice.http.api.ingestion.util.MultiCsvFilesConvertor;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0.CHUNK_ORIGIN;
import static com.hurence.historian.modele.HistorianServiceFields.*;
import static com.hurence.webapiservice.http.api.ingestion.ImportRequestParser.parseJsonImportRequest;
import static com.hurence.webapiservice.http.api.ingestion.util.IngestionApiUtil.fillingAllFilesReport;
import static com.hurence.webapiservice.http.api.ingestion.util.IngestionApiUtil.parseFiles;
import static com.hurence.webapiservice.http.api.ingestion.util.IngestionFinalResponseUtil.constructFinalResponseCsv;
import static com.hurence.webapiservice.http.api.ingestion.util.IngestionFinalResponseUtil.constructFinalResponseJson;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.CREATED;

public class IngestionApiImpl implements IngestionApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiImpl.class);
    private HistorianService service;

    public IngestionApiImpl(HistorianService service) {
        this.service = service;
    }

    /**
     * @param context        RoutingContext
     *
     * @return void
     */
    @Override
    public void importJson(RoutingContext context) {
        ImportRequestParser.CorrectPointsAndErrorMessages correctPointsAndErrorMessages;
        try {
            JsonArray jsonImportRequest = context.getBodyAsJsonArray();
            correctPointsAndErrorMessages = parseJsonImportRequest(jsonImportRequest);
        }catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage("BAD REQUEST");
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(errorObject.encodePrettily());
            return;
        }
        JsonObject pointsToBeInjected = new JsonObject().put(POINTS, correctPointsAndErrorMessages.correctPoints)
                .put(HistorianServiceFields.ORIGIN, "ingestion-json");

        service.rxAddTimeSeries(pointsToBeInjected)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "text/plain");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(response -> {
                    context.response().setStatusCode(CREATED);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(constructFinalResponseJson(response, correctPointsAndErrorMessages).encodePrettily());
                    LOGGER.info("response : {}", response);
                }).subscribe();
    }


    /**
     * @param context        RoutingContext
     *
     * @return void
     */
    @Override
    public void importCsv(RoutingContext context) {
        LOGGER.trace("received request at importCsv: {}", context.request());

        CsvFilesConvertorConf csvFilesConvertorConf;
        MultiCsvFilesConvertor multiCsvFilesConvertor = new MultiCsvFilesConvertor(context);

        try {
            csvFilesConvertorConf = new CsvFilesConvertorConf(context.request().formAttributes());
        } catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing attributes", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(String.valueOf(errorObject));
            return;
        }

        if (multiCsvFilesConvertor.uploads.isEmpty()) {
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(new JsonObject().put(ERRORS_RESPONSE_FIELD, "Request is not containing any files").encodePrettily());
            return;
        }

        try {
            parseFiles(multiCsvFilesConvertor.csvFileConvertors, multiCsvFilesConvertor.allFilesReport, csvFilesConvertorConf);
            fillingAllFilesReport(multiCsvFilesConvertor.csvFileConvertors, multiCsvFilesConvertor.allFilesReport);
        } catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(String.valueOf(errorObject));
            return;
        }

        if (multiCsvFilesConvertor.allFilesReport.correctPoints.isEmpty()) {
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(new JsonObject().put(ERRORS, multiCsvFilesConvertor.allFilesReport.filesThatFailedToBeImported).encodePrettily());
            return;
        }

        JsonObject pointsToBeInjected = new JsonObject().put(POINTS, multiCsvFilesConvertor.allFilesReport.correctPoints)
                .put(CHUNK_ORIGIN, "ingestion-csv");

        service.rxAddTimeSeries(pointsToBeInjected)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "text/plain");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(response -> {
                    context.response().setStatusCode(CREATED);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(constructFinalResponseCsv(multiCsvFilesConvertor.allFilesReport, csvFilesConvertorConf).encodePrettily());

                }).subscribe();
    }
}
