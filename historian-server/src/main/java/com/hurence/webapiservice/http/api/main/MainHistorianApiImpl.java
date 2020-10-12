package com.hurence.webapiservice.http.api.main;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hurence.historian.util.ErrorMsgHelper;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.models.ResponseAsList;
import com.hurence.webapiservice.http.api.grafana.modele.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginQueryRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.QueryRequestParser;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
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
                .end("Historian api is Working fine");
    }


    public final static String FROM_JSON_PATH = "/from";
    public final static String TO_JSON_PATH = "/to";
    public final static String NAMES_JSON_PATH = "/names";
    public final static String MAX_DATA_POINTS_JSON_PATH = "/max_data_points";
    public final static String FORMAT_JSON_PATH = "/format";
    public final static String TAGS_JSON_PATH = "/tags";
    public final static String SAMPLING_ALGO_JSON_PATH = "/sampling/algorithm";
    public final static String BUCKET_SIZE_JSON_PATH = "/sampling/bucket_size";
    public final static String REQUEST_ID_JSON_PATH = "/request_id";
    public final static String AGGREGATION_JSON_PATH = "/aggregations";
    public final static String QUALITY_JSON_PATH = "/quality";
    public final static String QUALITY_RETURN_JSON_PATH = "/return_with_quality";
    public final static String QUALITY_VALUE_JSON_PATH = QUALITY_JSON_PATH+"/quality_value";
    public final static String QUALITY_AGG_JSON_PATH = QUALITY_JSON_PATH+"/quality_agg";

    @Override
    public void export(RoutingContext context) {
        final long startRequest = System.currentTimeMillis();
        final HurenceDatasourcePluginQueryRequestParam request;
        try {
            final JsonObject requestBody = context.getBodyAsJson();
            /*
                When declaring QueryRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                    TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                    TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH,
                    AGGREGATION_JSON_PATH, QUALITY_VALUE_JSON_PATH, QUALITY_AGG_JSON_PATH, QUALITY_RETURN_JSON_PATH)
                    .parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.debug("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end(ErrorMsgHelper.createMsgError("Error parsing request !", ex));
            return;
        }

        int maxDataPoints = request.getSamplingConf().getMaxPoint();
        if (maxDataPointsAllowedForExportCsv < maxDataPoints ) {
            LOGGER.debug("error max data measures too large");
            context.response().setStatusCode(PAYLOAD_TOO_LARGE);
            context.response().setStatusMessage(StatusMessages.BAD_REQUEST);
            context.response().putHeader("Content-Type", "application/json");
            context.response().end("max data measures is bigger than allowed");
            return;
        }


        final JsonObject getTimeSeriesParams = buildGetTimeSeriesRequest(request);

        service
                .rxGetTimeSeries(getTimeSeriesParams)
                .map(sampledTimeSeries -> {
                    JsonArray timeseries = sampledTimeSeries.getJsonArray(TIMESERIES);
                    if (LOGGER.isDebugEnabled()) {
                        timeseries.forEach(metric -> {
                            JsonObject el = (JsonObject) metric;
                            String metricName = el.getString(FIELD_NAME);
                            int size = el.getJsonArray(TimeSeriesExtracterImpl.TIMESERIE_POINT).size();
                            LOGGER.debug("[REQUEST ID {}] return {} measures for metric {}.",
                                    request.getRequestId(),size, metricName);
                        });
                        LOGGER.debug("[REQUEST ID {}] Sampled a total of {} measures in {} ms.",
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

    private JsonObject buildGetTimeSeriesRequest(HurenceDatasourcePluginQueryRequestParam request) {
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(NAMES, request.getMetricNames())
                .put(FIELD_TAGS, request.getTags())
                .put(SAMPLING_ALGO, samplingConf.getAlgo())
                .put(BUCKET_SIZE, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC, samplingConf.getMaxPoint())
                .put(AGGREGATION, request.getAggs().stream().map(String::valueOf).collect(Collectors.toList()))
                .put(QUALITY_VALUE, request.getQualityValue())
                .put(QUALITY_AGG, request.getQualityAgg().toString())
                .put(QUALITY_RETURN, request.getQualityReturn())
                .put(USE_QUALITY, request.getUseQuality());
    }
}
