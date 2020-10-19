package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.ingestion.util.AllFilesReport;
import com.hurence.webapiservice.http.api.ingestion.util.CsvFileConvertor;
import com.hurence.webapiservice.http.api.ingestion.util.CsvFilesConvertorConf;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.SOLR_COLUMN_ORIGIN;
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
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(errorObject.encodePrettily());
            return;
        }
        JsonObject pointsToBeInjected = new JsonObject().put(POINTS, correctPointsAndErrorMessages.correctPoints)
                .put(SOLR_COLUMN_ORIGIN, "ingestion-json");

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

        //parse conf
        CsvFilesConvertorConf csvFilesConvertorConf;
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
        //throw 400 if no files
        if (context.fileUploads().isEmpty()) {
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(new JsonObject().put(ERRORS_RESPONSE_FIELD, "Request is not containing any files").encodePrettily());
            return;
        }

        //prepare to parse csv and the final report
        final List<CsvFileConvertor> csvFileConvertors = new ArrayList<>();
        context.fileUploads().forEach(file -> {
            CsvFileConvertor csvFileConvertor = new CsvFileConvertor(file);
            csvFileConvertors.add(csvFileConvertor);
        });
        final AllFilesReport allFilesReport = new AllFilesReport();
        //parse csv files and fill up report
        try {
            parseFiles(csvFileConvertors, allFilesReport, csvFilesConvertorConf);
            fillingAllFilesReport(csvFileConvertors, allFilesReport);
        } catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(String.valueOf(errorObject));
            return;
        }

        if (allFilesReport.correctPoints.isEmpty()) {
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(new JsonObject().put(ERRORS, allFilesReport.filesThatFailedToBeImported).encodePrettily());
            return;
        }

        JsonObject pointsToBeInjected = new JsonObject().put(POINTS, allFilesReport.correctPoints)
                .put(SOLR_COLUMN_ORIGIN, "ingestion-csv");

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
                    context.response().end(constructFinalResponseCsv(allFilesReport, csvFilesConvertorConf).encodePrettily());

                }).subscribe();
    }
}
