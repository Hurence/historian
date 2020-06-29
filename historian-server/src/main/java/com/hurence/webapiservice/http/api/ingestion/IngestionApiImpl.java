package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.ingestion.util.CsvFilesConvertorConf;
import com.hurence.webapiservice.http.api.ingestion.util.MultiCsvFilesConvertor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.ingestion.ImportRequestParser.parseJsonImportRequest;
import static com.hurence.webapiservice.http.api.ingestion.util.CsvConvertorUtil.fillingAllFilesReport;
import static com.hurence.webapiservice.http.api.ingestion.util.CsvConvertorUtil.parseFiles;
import static com.hurence.webapiservice.http.api.ingestion.util.IngestionApiUtil.*;
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
        JsonObject pointsToBeInjected = new JsonObject().put(POINTS_REQUEST_FIELD, correctPointsAndErrorMessages.correctPoints)
                .put(CHUNK_ORIGIN, "ingestion-json");

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

        CsvFilesConvertorConf csvFilesConvertorConf = new CsvFilesConvertorConf(context.request().formAttributes());
        MultiCsvFilesConvertor multiCsvFilesConvertor = new MultiCsvFilesConvertor(context);

        try {
            parseFiles(multiCsvFilesConvertor.csvFileConvertors, multiCsvFilesConvertor.allFilesReport, csvFilesConvertorConf);
            fillingAllFilesReport(multiCsvFilesConvertor.csvFileConvertors, multiCsvFilesConvertor.allFilesReport);
        } catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage("BAD REQUEST");
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(String.valueOf(errorObject));
            return;
        }
        JsonObject pointsToBeInjected = new JsonObject().put(POINTS_REQUEST_FIELD, multiCsvFilesConvertor.allFilesReport.correctPoints)
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
