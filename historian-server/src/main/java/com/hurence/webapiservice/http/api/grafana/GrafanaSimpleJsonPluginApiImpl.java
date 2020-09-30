package com.hurence.webapiservice.http.api.grafana;


import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.SearchRequestParam;
import com.hurence.webapiservice.http.api.grafana.parser.AnnotationRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.QueryRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.SearchRequestParser;
import com.hurence.webapiservice.http.api.modele.AnnotationRequest;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracter;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.hurence.historian.modele.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.NOT_FOUND;

public class GrafanaSimpleJsonPluginApiImpl implements GrafanaSimpleJsonPluginApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaSimpleJsonPluginApiImpl.class);
    protected HistorianService service;

    public final static String ALGO_TAG_KEY = "Algo";
    public final static String BUCKET_SIZE_TAG_KEY = "Bucket size";
    public final static List<String> FILTER_KEYS = Arrays.asList(ALGO_TAG_KEY, BUCKET_SIZE_TAG_KEY);
    public final static String TARGET = "target";
    private int maxDataPointsAllowed;

    public GrafanaSimpleJsonPluginApiImpl(HistorianService historianService, int maxDataPointsAllowed) {
        this.service = historianService;
        this.maxDataPointsAllowed = maxDataPointsAllowed;
    }


    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end("Historian grafana api is Working fine");
    }

    /**
     *  used by the find metric options on the query tab in panels.
     *  In our case we will return each different '{@value HistorianServiceFields#NAME}' value in historian.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     *      { target: 'upper_50' }
     * </pre>
     * response Exemple :
     * <pre>
     *     ["upper_25","upper_50","upper_75","upper_90","upper_95"]
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
            request = new SearchRequestParser("target", "limit").parseRequest(requestBody);
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
                    JsonArray array = metricResponse.getJsonArray(HistorianServiceFields.METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(array.encode());
                }).subscribe();
    }

    private JsonObject buildGetMetricsParam(SearchRequestParam request) {
        return new JsonObject()
                .put(HistorianServiceFields.METRIC, request.getStringToUseToFindMetrics())
                .put(HistorianServiceFields.LIMIT, request.getMaxNumberOfMetricNameToReturn());
    }

    /**
     *  used by the find metric options on the query tab in panels.
     *  In our case we will return each different '{@value HistorianServiceFields#NAME}' value in historian.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     *   {
     *     "panelId": 1,
     *     "range": {
     *         "from": "2016-10-31T06:33:44.866Z",
     *         "to": "2016-10-31T12:33:44.866Z",
     *         "raw": {
     *             "from": "now-6h",
     *             "to": "now"
     *         }
     *     },
     *     "rangeRaw": {
     *         "from": "now-6h",
     *         "to": "now"
     *     },
     *     "interval": "30s",
     *     "intervalMs": 30000,
     *     "targets": [
     *         { "target": "upper_50", "refId": "A", "type": "timeserie" },
     *         { "target": "upper_75", "refId": "B", "type": "timeserie" }
     *     ],
     *     "adhocFilters": [{
     *         "key": "City",
     *         "operator": "=",
     *         "value": "Berlin"
     *     }],
     *     "format": "json",
     *     "maxDataPoints": 550
     *   }
     * </pre>
     * response Exemple :
     * <pre>
     * [
     *   {
     *     "target":"upper_75",
     *     "datapoints":[
     *       [622,1450754160000],
     *       [365,1450754220000]
     *     ]
     *   },
     *   {
     *     "target":"upper_90",
     *     "datapoints":[
     *       [861,1450754160000],
     *       [767,1450754220000]
     *     ]
     *   }
     * ]
     * </pre>
     *
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     *          https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     *      </a>
     */
    @Override
    public void query(RoutingContext context) {
        final long startRequest = System.currentTimeMillis();
        final QueryRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            LOGGER.debug("requestBody : {}", requestBody.toString());
            /*
                When declaring QueryRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new QueryRequestParser().parseRequest(requestBody);
            if (request.getSamplingConf().getMaxPoint() > this.maxDataPointsAllowed) {
                throw new IllegalArgumentException(String.format("maximum allowed for %s is %s", "maxDataPoints", this.maxDataPointsAllowed));
            }
        } catch (Exception ex) {
            LOGGER.error("error parsing request", ex);
            context.response().setStatusCode(BAD_REQUEST);
            context.response().setStatusMessage(ex.getMessage());
            context.response().putHeader("Content-Type", "application/json");
            context.response().end();
            return;
        }

        final JsonObject getTimeSeriesChunkParams = buildHistorianRequest(request);
        LOGGER.debug("getTimeSeriesChunkParams : {}", getTimeSeriesChunkParams.toString());
        service
                .rxGetTimeSeries(getTimeSeriesChunkParams)
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
                    timeseries.forEach(timeserie -> {
                        JsonObject timeserieJson = (JsonObject) timeserie;
                        timeserieJson.put(TARGET, timeserieJson.getValue(FIELD_NAME));
                        timeserieJson.remove(FIELD_NAME);
                        timeserieJson.remove(TOTAL_POINTS);
                    });
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

    private JsonObject buildHistorianRequest(QueryRequestParam request) {
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




    /**
     *  used to the find annotations.
     * @param context
     *
     * Expected request exemple :
     * <pre>
     * {
     *   "range": {
     *     "from": "2016-04-15T13:44:39.070Z",
     *     "to": "2016-04-15T14:44:39.070Z"
     *   },
     *   "rangeRaw": {
     *     "from": "now-1h",
     *     "to": "now"
     *   },
     *   "annotation": {
     *     "name": "deploy",
     *     "datasource": "Simple JSON Datasource",
     *     "iconColor": "rgba(255, 96, 96, 1)",
     *     "enable": true,
     *     "query": "#deploy"
     *   }
     * }
     * </pre>
     * response Exemple :
     * <pre>
     * [
     *   {
     *     annotation: annotation, // The original annotation sent from Grafana.
     *     time: time, // Time since UNIX Epoch in milliseconds. (required)
     *     title: title, // The title for the annotation tooltip. (required)
     *     tags: tags, // Tags for the annotation. (optional)
     *     text: text // Text for the annotation. (optional)
     *   }
     * ]
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
            /*
                When declaring AnnotationRequestParser as a static variable, There is a problem parsing parallel requests
                at initialization (did not successfully reproduced this in a unit test).//TODO
             */
            request = new AnnotationRequestParser().parseRequest(requestBody);
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
                    context.response().end(modifyResponse(annotations).encode());
                }).subscribe();
    }

    private JsonArray modifyResponse(JsonObject annotationsRsp) {
        JsonArray annotationsAsArray = annotationsRsp.getJsonArray(HistorianServiceFields.ANNOTATIONS);
        annotationsAsArray.forEach(obj -> {
            modifyAnnotation((JsonObject) obj);
        });
        return annotationsAsArray;
    }
    private JsonObject modifyAnnotation(JsonObject annotation) {
        annotation.put("title", annotation.getString(HistorianServiceFields.TEXT));
        return annotation;
    }

    protected JsonObject buildHistorianAnnotationRequest(AnnotationRequest request) {
        return new JsonObject()
                .put(FROM, request.getFrom())
                .put(TO, request.getTo())
                .put(FIELD_TAGS, request.getTags())
                .put(LIMIT, request.getMaxAnnotation())
                .put(MATCH_ANY, request.getMatchAny())
                .put(TYPE, request.getType());
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
                .add(new JsonObject().put("text", SamplingAlgorithm.FIRST))
                .add(new JsonObject().put("text", SamplingAlgorithm.MIN))
                .add(new JsonObject().put("text", SamplingAlgorithm.MAX));
    }
}
