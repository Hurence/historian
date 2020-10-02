package com.hurence.webapiservice.http.api.main;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.models.ResponseAsList;
import com.hurence.webapiservice.http.api.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.QueryRequestParser;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracter;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.PAYLOAD_TOO_LARGE;

public class MainHistorianApiImpl implements MainHistorianApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainHistorianApiImpl.class);
    private HistorianService service;
    private int maxDataPointsAllowedForExportCsv;

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
            LOGGER.debug("error max data measures too large");
            context.response().setStatusCode(PAYLOAD_TOO_LARGE);
            context.response().setStatusMessage("max data measures is bigger than allowed");
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }


        final JsonObject getTimeSeriesParams = buildGetTimeSeriesRequest(request);

        service
                .rxGetTimeSeries(getTimeSeriesParams)
                .map(sampledTimeSeries -> {
                    JsonArray timeseries = sampledTimeSeries.getJsonArray(HistorianServiceFields.TIMESERIES);
                    if (LOGGER.isDebugEnabled()) {
                        timeseries.forEach(metric -> {
                            JsonObject el = (JsonObject) metric;
                            String metricName = el.getString(MultiTimeSeriesExtracter.TIMESERIE_NAME);
                            int size = el.getJsonArray(TimeSeriesExtracterImpl.TIMESERIE_POINT).size();
                            LOGGER.debug("[REQUEST ID {}] return {} measures for metric {}.",
                                    request.getRequestId(),size, metricName);
                        });
                        LOGGER.debug("[REQUEST ID {}] Sampled a total of {} measures in {} ms.",
                                request.getRequestId(),
                                sampledTimeSeries.getLong(HistorianServiceFields.TOTAL_POINTS, 0L),
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


    private JsonObject buildGetTimeSeriesRequest(QueryRequestParam request) {
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(HistorianServiceFields.FROM, request.getFrom())
                .put(HistorianServiceFields.TO, request.getTo())
                .put(HistorianServiceFields.NAMES, request.getMetricNames())
                .put(HistorianServiceFields.TAGS, request.getTags())
                .put(HistorianServiceFields.SAMPLING_ALGO, samplingConf.getAlgo())
                .put(HistorianServiceFields.BUCKET_SIZE, samplingConf.getBucketSize())
                .put(HistorianServiceFields.MAX_POINT_BY_METRIC, samplingConf.getMaxPoint());
    }
}
