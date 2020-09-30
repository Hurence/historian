package com.hurence.webapiservice.http.api.test;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.main.GetTimeSerieJsonRequestParser;
import com.hurence.webapiservice.http.api.main.GetTimeSerieRequestParam;
import com.hurence.webapiservice.modele.SamplingConf;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hurence.historian.modele.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;

public class TestHistorianApiImpl implements TestHistorianApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHistorianApiImpl.class);
    private HistorianService service;

    public TestHistorianApiImpl(HistorianService service) {
        this.service = service;
    }

    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian grafana api is Working fine");
    }

    @Override
    public void getTimeSerieChunks(RoutingContext context) {
        final GetTimeSerieRequestParam request;
        try {
            JsonObject body = context.getBodyAsJson();
            request =  new GetTimeSerieJsonRequestParser().parseRequest(body);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getTimeSeriesChunkParams = buildHistorianRequest(request);

        service
                .rxGetTimeSeriesChunk(getTimeSeriesChunkParams)
                .map(chunkResponse -> {
                    chunkResponse.getJsonArray(CHUNKS).forEach(chunk -> {
                        JsonObject chunkJson = (JsonObject) chunk;
                        chunkJson.remove("_version_");
                    });
                    return chunkResponse;
                })
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(timeseries -> {
                    JsonObject response = new JsonObject();
                    response
                            .put("query", context.getBodyAsJson())
                            .mergeIn(timeseries);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(response.encode());
                }).subscribe();
    }

    private JsonObject buildHistorianRequest(GetTimeSerieRequestParam request) {
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(NAMES, request.getMetricNames())
                .put(FIELD_TAGS, request.getTags())
                .put(SAMPLING_ALGO, samplingConf.getAlgo())
                .put(BUCKET_SIZE, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC, samplingConf.getMaxPoint());
    }

}
