package com.hurence.webapiservice.http.ingestion;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.subscribers.DisposableSubscriber;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.FileUpload;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.Codes.BAD_REQUEST;
import static com.hurence.webapiservice.http.Codes.CREATED;


public class IngestionApiImpl implements IngestionApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionApiImpl.class);
    private HistorianService service;

    public IngestionApiImpl(HistorianService service) {
        this.service = service;

    }

    @Override
    public void importJson(RoutingContext context) {
        final ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder;
        try {
            JsonArray getMetricsParam = context.getBodyAsJsonArray();
             responseAndErrorHolder = new ImportRequestParser().checkAndBuildValidHistorianImportRequest(getMetricsParam);
        }catch (Exception ex) {
            JsonObject errorObject = new JsonObject().put(ERRORS_RESPONSE_FIELD, ex.getMessage());
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage("BAD REQUEST");
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(String.valueOf(errorObject));
            return;
        }

        service.rxAddTimeSeries(responseAndErrorHolder.correctPoints)
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
                    context.response().end(constructFinalResponse(response, responseAndErrorHolder).encode());
                    LOGGER.info("response : {}", response);
                }).subscribe();
    }

    private JsonObject constructFinalResponse(JsonObject response, ImportRequestParser.ResponseAndErrorHolder responseAndErrorHolder) {
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
        //TODO finish this method !!!
        LOGGER.trace("received request at importCsv: {}", context.request());
        Set<FileUpload> uploads = context.fileUploads();


        List<Single<JsonObject>> importedFiles = uploads.stream()
                .map(fileUpload -> this.startCsvImportJob(context, fileUpload))
                .collect(Collectors.toList());

        Single.zip(importedFiles, a -> "OK")
        .subscribe(result -> {
            LOGGER.info("import finished with zip !!!!");
        });
        context.response().setStatusCode(200);
        context.response().putHeader("Content-Type", "application/json");
        context.response().end(new JsonObject().put("status", "OK").encode());
    }

    /**
     *
     * @param context
     * @param fileUpload
     * @return
     */
    private Single<JsonObject> startCsvImportJob(RoutingContext context, final FileUpload fileUpload) {
        LOGGER.trace("uploaded file : {} of size : {}", fileUpload.fileName(), fileUpload.size());
        LOGGER.trace("contentType file : {} of contentTransferEncoding : {}", fileUpload.contentType(), fileUpload.contentTransferEncoding());
        LOGGER.trace("uploaded uploadedFileName : {} ", fileUpload.uploadedFileName());
        LOGGER.info("uploaded charSet : {} ", fileUpload.charSet());
        OpenOptions options = new OpenOptions();

        return context.vertx().fileSystem().rxOpen(fileUpload.uploadedFileName(), options)
                .map(file -> {
                    Flowable<Buffer> flowable = file.toFlowable();
                    flowable
                            .delay(1, TimeUnit.SECONDS)
                            .subscribeWith(getChunkingDisposable());
//                            .forEach(data -> LOGGER.info("Read data: " + data.toString("UTF-8")));
//                    LOGGER.info("imported csv");
                    return new JsonObject()
                            .put("file", fileUpload.name())
                            .put("status", "running");
                });
    }

   private DisposableSubscriber getChunkingDisposable() {
        return new DisposableSubscriber<Buffer>() {
            @Override public void onStart() {
                LOGGER.info("Start!");
                request(1);
            }
            @Override public void onNext(Buffer data) {
                LOGGER.info("Read data: " + data.toString("UTF-8"));
                request(1);
            }
            @Override public void onError(Throwable t) {
                t.printStackTrace();
            }
            @Override public void onComplete() {
                LOGGER.info("Done!");
//                                    testContext.completeNow();
            }
        };
    }
}
