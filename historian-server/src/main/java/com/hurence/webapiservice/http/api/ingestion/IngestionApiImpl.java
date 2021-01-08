package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.ingestion.util.AllFilesReport;
import com.hurence.webapiservice.http.api.ingestion.util.CsvFileConvertor;
import com.hurence.webapiservice.http.api.ingestion.util.CsvFilesConvertorConf;
import com.hurence.webapiservice.http.api.modele.ContentType;
import com.hurence.webapiservice.http.api.modele.Headers;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.util.VertxErrorAnswerDescription;
import com.hurence.webapiservice.util.VertxHttpErrorMsgHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.model.HistorianServiceFields.ERRORS;
import static com.hurence.historian.model.HistorianServiceFields.POINTS;
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
            LOGGER.info("Error parsing request :", ex);
            VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                    .errorMsg("Error parsing request")
                    .statusCode(BAD_REQUEST)
                    .statusMsg(StatusMessages.BAD_REQUEST)
                    .throwable(ex)
                    .routingContext(context)
                    .build();
            VertxHttpErrorMsgHelper.answerWithError(error);
            return;
        }
        JsonObject pointsToBeInjected = new JsonObject().put(POINTS, correctPointsAndErrorMessages.correctPoints)
                .put(SOLR_COLUMN_ORIGIN, "ingestion-json");

        service.rxAddTimeSeries(pointsToBeInjected)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error :", ex);
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .errorMsg("Unexpected error")
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(response -> {
                    context.response().setStatusCode(CREATED)
                            .setStatusMessage(StatusMessages.CREATED)
                            .putHeader(Headers.contentType, ContentType.APPLICATION_JSON.contentType)
                            .end(constructFinalResponseJson(response, correctPointsAndErrorMessages).encodePrettily());
                    LOGGER.debug("response : {}", response);
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
            LOGGER.info("Error parsing request :", ex);
            VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                    .errorMsg("Error parsing request")
                    .statusCode(BAD_REQUEST)
                    .statusMsg(StatusMessages.BAD_REQUEST)
                    .throwable(ex)
                    .routingContext(context)
                    .build();
            VertxHttpErrorMsgHelper.answerWithError(error);
            return;
        }
        //throw 400 if no files
        if (context.fileUploads().isEmpty()) {
            LOGGER.info("Request is not containing any files");
            VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                    .errorMsg("Request is not containing any files")
                    .statusCode(BAD_REQUEST)
                    .statusMsg(StatusMessages.BAD_REQUEST)
                    .routingContext(context)
                    .build();
            VertxHttpErrorMsgHelper.answerWithError(error);
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
            LOGGER.info("Error parsing request !", ex);
            VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                    .errorMsg("Error parsing request !")
                    .statusCode(BAD_REQUEST)
                    .statusMsg(StatusMessages.BAD_REQUEST)
                    .throwable(ex)
                    .routingContext(context)
                    .build();
            VertxHttpErrorMsgHelper.answerWithError(error);
            return;
        }

        if (allFilesReport.correctPoints.isEmpty()) {
            LOGGER.info("No valid points available !");
            context.response().setStatusCode(BAD_REQUEST)
                    .setStatusMessage(StatusMessages.BAD_REQUEST)
                    .putHeader(Headers.contentType, ContentType.APPLICATION_JSON.contentType)
                    .end(new JsonObject().put(ERRORS, allFilesReport.filesThatFailedToBeImported).encodePrettily());
            return;
        }

        JsonObject pointsToBeInjected = new JsonObject().put(POINTS, allFilesReport.correctPoints)
                .put(SOLR_COLUMN_ORIGIN, "ingestion-csv");

        service.rxAddTimeSeries(pointsToBeInjected)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error in importCsv(RoutingContext)", ex);
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(response -> {
                    context.response().setStatusCode(CREATED)
                            .setStatusMessage(StatusMessages.CREATED)
                            .putHeader(Headers.contentType, ContentType.APPLICATION_JSON.contentType)
                            .end(constructFinalResponseCsv(allFilesReport, csvFilesConvertorConf).encodePrettily());
                }).subscribe();
    }
}
