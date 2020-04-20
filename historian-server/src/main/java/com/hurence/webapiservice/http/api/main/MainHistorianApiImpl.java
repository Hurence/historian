package com.hurence.webapiservice.http.api.main;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.HistorianResponseHelper;
import com.hurence.webapiservice.historian.util.models.ResponseAsList;
import com.hurence.webapiservice.http.GetTimeSerieRequestParser;
import com.hurence.webapiservice.http.api.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.QueryRequestParser;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.LogislandTimeSeriesModeler;
import com.hurence.webapiservice.timeseries.TimeSeriesExtracterImpl;
import com.hurence.webapiservice.timeseries.TimeSeriesModeler;
import com.hurence.webapiservice.timeseries.TimeSeriesRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.Codes.BAD_REQUEST;
import static com.hurence.webapiservice.http.Codes.PAYLOAD_TOO_LARGE;

public class MainHistorianApiImpl implements MainHistorianApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainHistorianApiImpl.class);
    private HistorianService service;
    private int maxDataPointsAllowedForExportCsv;

    private static final GetTimeSerieRequestParser getTimeSerieParser = new GetTimeSerieRequestParser();
    private static TimeSeriesModeler timeserieModeler = new LogislandTimeSeriesModeler();

    public MainHistorianApiImpl(HistorianService service, int maxDataPointsAllowedForExportCsv) {
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
                    JsonArray array = metricResponse.getJsonArray(METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(array.encode());
                }).subscribe();
    }

    @Override
    public void getTimeSeries(RoutingContext context) {
        final TimeSeriesRequest request;
        try {
            MultiMap map = context.queryParams();
            request = getTimeSerieParser.parseRequest(map);
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
                    List<JsonObject> chunks = HistorianResponseHelper.extractChunks(chunkResponse);
                    Map<String, List<JsonObject>> chunksByName = chunks.stream().collect(
                            Collectors.groupingBy(chunk ->  chunk.getString(RESPONSE_METRIC_NAME_FIELD))
                    );
                    return TimeSeriesModeler.buildTimeSeries(request, chunksByName, timeserieModeler);
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
                            .put("query", "TODO")
                            .put("total_timeseries", "TODO")
                            .put("timeseries", timeseries);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(response.encode());
                }).subscribe();
    }

    private JsonObject buildHistorianRequest(TimeSeriesRequest request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(RESPONSE_CHUNK_VALUE_FIELD)
                .add(RESPONSE_CHUNK_START_FIELD)
                .add(RESPONSE_CHUNK_END_FIELD)
                .add(RESPONSE_CHUNK_SIZE_FIELD)
                .add(RESPONSE_METRIC_NAME_FIELD);
        request.getAggs().forEach(agg -> {
            final String aggField;
            switch (agg) {
                case MIN:
                    aggField = RESPONSE_CHUNK_MIN_FIELD;
                    break;
                case MAX:
                    aggField = RESPONSE_CHUNK_MAX_FIELD;
                    break;
                case AVG:
                    aggField = RESPONSE_CHUNK_AVG_FIELD;
                    break;
                case COUNT:
                    aggField = RESPONSE_CHUNK_SIZE_FIELD;
                    break;
                case SUM:
                    aggField = RESPONSE_CHUNK_SUM_FIELD;
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
            fieldsToFetch.add(aggField);
        });
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(FIELDS, fieldsToFetch)
                .put(NAMES, request.getMetricNames())
                .put(HistorianFields.TAGS, request.getTags())
                .put(SAMPLING_ALGO, samplingConf.getAlgo())
                .put(BUCKET_SIZE, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC, samplingConf.getMaxPoint());
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
                    JsonArray timeseries = sampledTimeSeries.getJsonArray(TIMESERIES);
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
                                sampledTimeSeries.getLong(TOTAL_POINTS, 0L),
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

}
