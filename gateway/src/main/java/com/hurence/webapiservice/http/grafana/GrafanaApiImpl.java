package com.hurence.webapiservice.http.grafana;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.models.ResponseAsList;
import com.hurence.webapiservice.http.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.AnnotationRequest;
import com.hurence.webapiservice.timeseries.TimeSeriesExtracterImpl;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.Codes.*;

public class GrafanaApiImpl implements GrafanaApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaApiImpl.class);
    private HistorianService service;
    private int maxDataPointsAllowedForExportCsv;

    public final static String ALGO_TAG_KEY = "Algo";
    public final static String BUCKET_SIZE_TAG_KEY = "Bucket size";
    public final static String FILTER_TAG_KEY = "Tag";
    public final static String MAX_POINT_REQUEST_FIELD = "maxDataPoints";


    public GrafanaApiImpl(HistorianService service, int maxDataPointsAllowedForExportCsv) {
        this.service = service;
        this.maxDataPointsAllowedForExportCsv = maxDataPointsAllowedForExportCsv;

    }


    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian grafana api is Working fine");
    }

    @Override
    public void search(RoutingContext context) {
        final JsonObject getMetricsParam = context.getBodyAsJson();

        service.rxGetMetricsName(getMetricsParam)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(metricResponse -> {
                    JsonArray array = metricResponse.getJsonArray(RESPONSE_METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(array.encode());
                }).subscribe();
    }

    @Override
    public void query(RoutingContext context) {
        final long startRequest = System.currentTimeMillis();
        final QueryRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            /*
                When declaring QueryRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new QueryRequestParser().parseRequest(requestBody);
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
                .rxGetTimeSeries(getTimeSeriesChunkParams)
                .map(sampledTimeSeries -> {
                    JsonArray timeseries = sampledTimeSeries.getJsonArray(TIMESERIES_RESPONSE_FIELD);
                    if (LOGGER.isDebugEnabled()) {
                        timeseries.forEach(metric -> {
                            JsonObject el = (JsonObject) metric;
                            String metricName = el.getString(TimeSeriesExtracterImpl.TIMESERIE_NAME);
                            int size = el.getJsonArray(TimeSeriesExtracterImpl.TIMESERIE_POINT).size();
                            LOGGER.debug("[REQUEST ID {}] return {} points for metric {}.",
                                    request.getRequestId(),size, metricName);
                        });
                        LOGGER.debug("[REQUEST ID {}] Sampled a total of {} points in {} ms.",
                                request.getRequestId(),
                                sampledTimeSeries.getLong(TOTAL_POINTS_RESPONSE_FIELD, 0L),
                                System.currentTimeMillis() - startRequest);
                    }
                    return timeseries;
                })
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(timeseries -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(timeseries.encode());
                    LOGGER.debug("body :: {}", timeseries);
                }).subscribe();
    }

    @Override
    public void export(RoutingContext context) {
        final long startRequest = System.currentTimeMillis();
        final QueryRequestParam request;
        try {
            final JsonObject requestBody = context.getBodyAsJson();
            /*
                When declaring QueryRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new QueryRequestParser().parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.debug("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        int maxDataPoints = request.getMaxDataPoints();
        if (maxDataPointsAllowedForExportCsv < maxDataPoints ) {
            LOGGER.debug("error max data points too large");
            context.response().setStatusCode(PAYLOAD_TOO_LARGE);
            context.response().setStatusMessage("max data points is bigger than allowed");
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }


        final JsonObject getTimeSeriesChunkParams = buildHistorianRequest(request);

        service
                .rxGetTimeSeries(getTimeSeriesChunkParams)
                .map(sampledTimeSeries -> {
                    JsonArray timeseries = sampledTimeSeries.getJsonArray(TIMESERIES_RESPONSE_FIELD);
                    if (LOGGER.isDebugEnabled()) {
                        timeseries.forEach(metric -> {
                            JsonObject el = (JsonObject) metric;
                            String metricName = el.getString(TimeSeriesExtracterImpl.TIMESERIE_NAME);
                            int size = el.getJsonArray(TimeSeriesExtracterImpl.TIMESERIE_POINT).size();
                            LOGGER.debug("[REQUEST ID {}] return {} points for metric {}.",
                                    request.getRequestId(),size, metricName);
                        });
                        LOGGER.debug("[REQUEST ID {}] Sampled a total of {} points in {} ms.",
                                request.getRequestId(),
                                sampledTimeSeries.getLong(TOTAL_POINTS_RESPONSE_FIELD, 0L),
                                System.currentTimeMillis() - startRequest);
                    }
                    return timeseries;
                })
                .map(timeseries -> {
                    ResponseAsList responseAsList = new ResponseAsList(timeseries);
                    List<ResponseAsList.SubResponse> list = responseAsList.ReturnList();
                    CsvSchema schema = CsvSchema.builder()
                            .addColumn("metric")
                            .addColumn("value")
                            .addColumn("date")
                            .build();
                    CsvMapper csvMapper = new CsvMapper();
                    csvMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN,true);
                    /*File file = new File("src/main/resources/results.csv");
                    csvMapper.writerFor(ArrayList.class)
                            .with(schema.withUseHeader(true))
                            .writeValue(file, list);*/
                    String csv = csvMapper.writerFor(ArrayList.class)
                            .with(schema.withUseHeader(true)).writeValueAsString(list);
                    return csv;
                })
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "text/csv");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(timeseries -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "text/csv");
                    context.response().end(timeseries);
                }).subscribe();
    }



    private JsonObject buildHistorianRequest(TimeSeriesRequest request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(RESPONSE_CHUNK_VALUE_FIELD)
                .add(RESPONSE_CHUNK_START_FIELD)
                .add(RESPONSE_CHUNK_END_FIELD)
                .add(RESPONSE_CHUNK_SIZE_FIELD)
                .add(RESPONSE_METRIC_NAME_FIELD);
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(FROM_REQUEST_FIELD, request.getFrom())
                .put(TO_REQUEST_FIELD, request.getTo())
                .put(FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD, fieldsToFetch)
                .put(METRIC_NAMES_AS_LIST_REQUEST_FIELD, request.getMetricNames())
                .put(TAGS_TO_FILTER_ON_REQUEST_FIELD, request.getTags())
                .put(SAMPLING_ALGO_REQUEST_FIELD, samplingConf.getAlgo())
                .put(BUCKET_SIZE_REQUEST_FIELD, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC_REQUEST_FIELD, samplingConf.getMaxPoint());
    }

    private JsonObject buildHistorianAnnotationRequest(AnnotationRequest request) {
        return new JsonObject()
                .put(FROM_REQUEST_FIELD, request.getFrom())
                .put(TO_REQUEST_FIELD, request.getTo())
                .put(TAGS_REQUEST_FIELD, request.getTagsAsJsonArray())
                .put(MAX_ANNOTATION_REQUEST_FIELD, request.getMaxAnnotation())
                .put(MATCH_ANY_REQUEST_FIELD, request.getMatchAny())
                .put(TYPE_REQUEST_FIELD, request.getType());
    }

    @Override
    public void annotations(RoutingContext context) {
        final AnnotationRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            /*
                When declaring AnnotationRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new AnnotationRequestParser().parseAnnotationRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getAnnotationParams = buildHistorianAnnotationRequest(request);

        service
                .rxGetAnnotations(getAnnotationParams)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(annotations -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(annotations.encode());

                }).subscribe();
    }

    /**
     * return every custom key parameters that can be used to query data.
     * @param context
     */
    @Override
    public void tagKeys(RoutingContext context) {
        context.response().setStatusCode(200);
        context.response().putHeader("Content-Type", "application/json");
        context.response().end(new JsonArray()
                .add(new JsonObject().put("type", "string").put("text", ALGO_TAG_KEY))
                .add(new JsonObject().put("type", "int").put("text", BUCKET_SIZE_TAG_KEY))
                .add(new JsonObject().put("type", "string").put("text", FILTER_TAG_KEY))
                .encode()
        );
    }
    /**
     * return every custom value parameters given a key that can be used to query data.
     * @param context
     */
    @Override
    public void tagValues(RoutingContext context) {
        final String keyValue;
        try {
            keyValue = parseTagValuesRequest(context);
        } catch (IllegalArgumentException ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }
        final JsonArray response;
        switch (keyValue) {
            case ALGO_TAG_KEY:
                response = getTagValuesOfAlgo();
                break;
            case BUCKET_SIZE_TAG_KEY:
                //TODO verify how to handle integer type
                response = new JsonArray()
                        .add(new JsonObject().put("text", "50"))
                        .add(new JsonObject().put("text", "100"))
                        .add(new JsonObject().put("text", "250"))
                        .add(new JsonObject().put("text", "500"));
                break;
            case FILTER_TAG_KEY:
                response = new JsonArray()
                        .add(new JsonObject().put("text", "your tag"));
            default:
                LOGGER.warn("there is no tag with this key !");
                context.response().setStatusCode(NOT_FOUND);
                context.response().setStatusMessage("there is no tag with this key !");
                context.response().putHeader("Content-Type", "application/json");
                context.response().end();
                return;
        }
        context.response().setStatusCode(200);
        context.response().putHeader("Content-Type", "application/json");
        context.response().end(response.encode());
    }

    private String  parseTagValuesRequest(RoutingContext context) throws IllegalArgumentException {
        JsonObject body = context.getBodyAsJson();
        try {
            return body.getString("key");
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("body request does not contain a key 'key'. " +
                    "Request is expected to be the following format : %s \n\n but was %s",
                    "{ \"key\":\"Algo\"}", body.encodePrettily()));
        }
    }

    private JsonArray getTagValuesOfAlgo() {
        return new JsonArray()
                .add(new JsonObject().put("text", SamplingAlgorithm.NONE))
                .add(new JsonObject().put("text", SamplingAlgorithm.AVERAGE))
                .add(new JsonObject().put("text", SamplingAlgorithm.FIRST_ITEM))
                .add(new JsonObject().put("text", SamplingAlgorithm.MIN))
                .add(new JsonObject().put("text", SamplingAlgorithm.MAX));
    }
}
