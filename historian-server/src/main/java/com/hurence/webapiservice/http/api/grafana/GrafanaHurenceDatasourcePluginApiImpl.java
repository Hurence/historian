package com.hurence.webapiservice.http.api.grafana;


import com.hurence.historian.modele.HistorianFields;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.RefIdInfo;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.SearchRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.SearchValuesRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginAnnotationRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginQueryRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.SearchRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.SearchValuesRequestParser;
import com.hurence.webapiservice.http.api.modele.AnnotationRequest;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterImpl;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.http.api.main.modele.QueryFields.QUERY_PARAM_REF_ID;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;
import static com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracter.TIMESERIE_NAME;

public class GrafanaHurenceDatasourcePluginApiImpl implements GrafanaHurenceDatasourcePluginApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaHurenceDatasourcePluginApiImpl.class);

    protected HistorianService service;

    public GrafanaHurenceDatasourcePluginApiImpl(HistorianService service) {
        this.service = service;
    }


    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian grafana api is Working fine");
    }

    /**
     *  used by the find metric options on the query tab in panels.
     *  In our case we will return each different '{@value HistorianFields#NAME}' value in historian.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     *      {
     *         "name": "metric_1",
     *         "limit": 100
     *      }
     * </pre>
     * "limit" is optional.
     *
     * response Example :
     * <pre>
     *     ["metric_25","metric_50","metric_75","metric_90","metric_95"]
     * </pre>
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     */
    @Override
    public void search(RoutingContext context) {
        final SearchRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            /*
                When declaring QueryRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new SearchRequestParser("name", "limit").parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }
        final JsonObject getMetricsParam = buildGetMetricsParam(request);

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

    private JsonObject buildGetMetricsParam(SearchRequestParam request) {
        return new JsonObject()
                .put(METRIC, request.getStringToUseToFindMetrics())
                .put(LIMIT, request.getMaxNumberOfMetricNameToReturn());
    }

    /**
     *  used to get values of certain field
     *
     * @param context
     *
     * Expected request exemple :
     * <pre>
     *      {
     *         "field": "name",
     *         "query": "met",
     *         "limit": 100
     *      }
     * </pre>
     * "query" is optional.
     * "limit" is optional.
     *
     * response Example :
     * <pre>
     *     ["metric_25","metric_50","metric_75","metric_90","metric_95"]
     * </pre>
     *
     */
    @Override
    public void searchValues(RoutingContext context) {
        final SearchValuesRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();

            request = new SearchValuesRequestParser(FIELD, QUERY, LIMIT).parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }
        final JsonObject getFieldValuesParam = buildGetFieldValuesParam(request);

        service.rxGetFieldValues(getFieldValuesParam)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(valuesResponse -> {
                    JsonArray array = valuesResponse.getJsonArray(RESPONSE_VALUES);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(array.encode());
                }).subscribe();
    }


    private JsonObject buildGetFieldValuesParam(SearchValuesRequestParam request) {
        return new JsonObject()
                .put(FIELD, request.getFieldToSearch())
                .put(QUERY, request.getQueryToUseInSearch())
                .put(LIMIT, request.getMaxNumberOfMetricNameToReturn());
    }

    @Override
    public void searchTags(RoutingContext context) {
        service.rxGetTagNames()
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    context.response().setStatusCode(500);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(ex.getMessage());
                })
                .doOnSuccess(tagsList -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(tagsList.encode());
                }).subscribe();
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

    /**
     *  used to query metrics datapoints in grafana panels.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     *   {
     *     "from": "2016-10-31T06:33:44.866Z",
     *     "to": "2020-10-31T12:33:44.866Z",
     *     "names": ["metric_1"],
     *     "format": "json",
     *     "max_data_points": 8,
     *     "tags": {
     *         "sensor" : "sensor_1"
     *     },
     *     "sampling":{
     *        "algorithm": "MIN",
     *        "bucket_size" : 100
     *     }
     *   }
     * </pre>
     * response Exemple :
     * <pre>
     * [
     *   {
     *     "target":"upper_75",
     *     "tags" : {
     *         "sensor" : "sensor_1"
     *     },
     *     "datapoints":[
     *       [622,1450754160000],
     *       [365,1450754220000]
     *     ]
     *   },
     *   {
     *     "target":"upper_90",
     *     "tags" : {
     *        "sensor" : "sensor_1"
     *     },
     *     "datapoints":[
     *       [861,1450754160000],
     *       [767,1450754220000]
     *     ]
     *   }
     * ]
     * </pre>
     *
     * le champs "tags" n'est retourné que si présent dans la requête.
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     */
    @Override
    public void query(RoutingContext context) {
        final long startRequest = System.currentTimeMillis();
        final HurenceDatasourcePluginQueryRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            LOGGER.debug("requestBody : {}", requestBody.toString());
            /*
                When declaring QueryRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new HurenceDatasourcePluginQueryRequestParser(FROM_JSON_PATH,
                    TO_JSON_PATH,NAMES_JSON_PATH, MAX_DATA_POINTS_JSON_PATH,FORMAT_JSON_PATH,
                    TAGS_JSON_PATH,SAMPLING_ALGO_JSON_PATH,BUCKET_SIZE_JSON_PATH, REQUEST_ID_JSON_PATH, AGGREGATION_JSON_PATH)
                    .parseRequest(requestBody);
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getTimeSeriesChunkParams = buildGetTimeSeriesRequest(request);
        LOGGER.debug("getTimeSeriesChunkParams : {}", getTimeSeriesChunkParams.toString());
        Single<JsonObject> rsp = service.rxGetTimeSeries(getTimeSeriesChunkParams);
        rsp = logThingsIfDebugMode(startRequest, request, rsp);
        rsp.map(timeseries -> {
                    JsonArray sampledTimeSeries = timeseries.getJsonArray(TIMESERIES);
                    List<RefIdInfo> refIdList = getRefIdInfos(request);
                    List<JsonObject> timeseriesAsList = sampledTimeSeries.stream().map(timeserieWithoutRefId -> {
                        JsonObject timeserieWithRefIdIfExist = (JsonObject) timeserieWithoutRefId;
                        addRefIdIfExist(refIdList, timeserieWithRefIdIfExist);
                        return timeserieWithRefIdIfExist;
                    }).collect(Collectors.toList());
                    return new JsonArray(timeseriesAsList);
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

    private Single<JsonObject> logThingsIfDebugMode(long startRequest, HurenceDatasourcePluginQueryRequestParam request, Single<JsonObject> rsp) {
        if (LOGGER.isDebugEnabled()) {
            rsp = rsp.map(sampledTimeSeriesRsp -> {
                if (LOGGER.isDebugEnabled()) {
                    JsonArray timeseries = sampledTimeSeriesRsp.getJsonArray(TIMESERIES);
                    timeseries.forEach(metric -> {
                        JsonObject el = (JsonObject) metric;
                        String metricName = el.getString(TIMESERIE_NAME);
                        int size = el.getJsonArray(TimeSeriesExtracterImpl.TIMESERIE_POINT).size();
                        LOGGER.debug("[REQUEST ID {}] return {} points for metric {}.",
                                request.getRequestId(),size, metricName);
                    });
                    LOGGER.debug("[REQUEST ID {}] Sampled a total of {} points in {} ms.",
                            request.getRequestId(),
                            sampledTimeSeriesRsp.getLong(TOTAL_POINTS, 0L),
                            System.currentTimeMillis() - startRequest);
                }
                return sampledTimeSeriesRsp;
            });
        }
        return rsp;
    }

    private void addRefIdIfExist(List<RefIdInfo> refIdList, JsonObject timeserieWithoutRefId) {
        for (RefIdInfo refIdInfo : refIdList){
            if (refIdInfo.isMetricMatching(timeserieWithoutRefId)) {
                timeserieWithoutRefId.put(QUERY_PARAM_REF_ID, refIdInfo.getRefId());
            }
        }
    }

    private List<RefIdInfo> getRefIdInfos(HurenceDatasourcePluginQueryRequestParam request) {
        List<RefIdInfo> refIdList = new ArrayList<>();
        for (Object metricInfo : request.getMetricNames()) {
            if (metricInfo instanceof JsonObject && ((JsonObject) metricInfo).containsKey(QUERY_PARAM_REF_ID)) {
                JsonObject metricInfoObject = new JsonObject(metricInfo.toString());
                Map<String, String> finalTagsForThisMetric = new HashMap<>(request.getTags());
                if (metricInfoObject.containsKey(TAGS))
                    for (Map.Entry<String, Object> tagsEntry : metricInfoObject.getJsonObject(TAGS).getMap().entrySet()){
                        finalTagsForThisMetric.put(tagsEntry.getKey() ,tagsEntry.getValue().toString());
                    }
                refIdList.add(new RefIdInfo(metricInfoObject.getString(NAME),
                        metricInfoObject.getString(QUERY_PARAM_REF_ID),
                        finalTagsForThisMetric));
            }
        }
        return refIdList;
    }

    private JsonObject buildGetTimeSeriesRequest(HurenceDatasourcePluginQueryRequestParam request) {
        JsonArray fieldsToFetch = new JsonArray()
                .add(CHUNK_VALUE_FIELD)
                .add(CHUNK_START_FIELD)
                .add(CHUNK_END_FIELD)
                .add(CHUNK_COUNT_FIELD)
                .add(NAME);
        SamplingConf samplingConf = request.getSamplingConf();
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(FIELDS, fieldsToFetch)
                .put(NAMES, request.getMetricNames())
                .put(HistorianFields.TAGS, request.getTags())
                .put(SAMPLING_ALGO, samplingConf.getAlgo())
                .put(BUCKET_SIZE, samplingConf.getBucketSize())
                .put(MAX_POINT_BY_METRIC, samplingConf.getMaxPoint())
                .put(AGGREGATION, request.getAggs().stream().map(String::valueOf).collect(Collectors.toList()));
    }

    public final static String LIMIT_JSON_PATH = "/limit";
    public final static String MATCH_ANY_JSON_PATH = "/matchAny";
    public final static String TYPE_JSON_PATH = "/type";

    /**
     *  used to the find annotations.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     * {
     *     "from": "2020-2-14T01:43:14.070Z",
     *     "to": "2020-2-14T06:50:14.070Z",
     *     "limit" : 100,
     *     "tags": ["tag1", "tag2"],
     *     "matchAny": false,
     *     "type": "tags"
     * }
     * </pre>
     * response Exemple :
     * <pre>
     * {
     *   "annotations" : [
     *     {
     *       "time": 1581648194070,
     *       "text": "annotation 1",
     *       "tags": ["tag1","tag2"]
     *     }
     *   ],
     *   "total_hit" : 1
     * }
     * </pre>
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     */
    @Override
    public void annotations(RoutingContext context) {
        final AnnotationRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            request = new HurenceDatasourcePluginAnnotationRequestParser(
                    FROM_JSON_PATH, TO_JSON_PATH, TAGS_JSON_PATH, TYPE_JSON_PATH, LIMIT_JSON_PATH, MATCH_ANY_JSON_PATH
            ).parseRequest(requestBody);
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

    protected JsonObject buildHistorianAnnotationRequest(AnnotationRequest request) {
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(TAGS, request.getTags())
                .put(LIMIT, request.getMaxAnnotation())
                .put(MATCH_ANY, request.getMatchAny())
                .put(TYPE, request.getType());
    }
}
