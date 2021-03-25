package com.hurence.webapiservice.http.api.grafana.promql.router;


import com.hurence.historian.model.HistorianServiceFields;
import com.hurence.webapiservice.historian.models.RefIdInfo;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.http.api.grafana.model.*;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginAnnotationRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.HurenceDatasourcePluginQueryRequestParser;
import com.hurence.webapiservice.http.api.grafana.parser.SearchValuesRequestParser;
import com.hurence.webapiservice.http.api.grafana.promql.request.LabelsRequest;
import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;
import com.hurence.webapiservice.http.api.modele.AnnotationRequest;
import com.hurence.webapiservice.http.api.modele.StatusMessages;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.util.VertxErrorAnswerDescription;
import com.hurence.webapiservice.util.VertxHttpErrorMsgHelper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
import static com.hurence.webapiservice.http.api.main.modele.QueryFields.QUERY_PARAM_REF_ID;
import static com.hurence.webapiservice.http.api.modele.StatusCodes.BAD_REQUEST;
import static com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracter.TIMESERIE_POINT;

/**
 * {
 * "status": "success" | "error",
 * "data": <data>,
 * <p>
 * // Only set if status is "error". The data field may still hold
 * // additional data.
 * "errorType": "<string>",
 * "error": "<string>",
 * <p>
 * // Only if there were warnings while executing the request.
 * // There will still be data in the data field.
 * "warnings": ["<string>"]
 * }
 */
public class GrafanaPromQLDatasourcePluginApiImpl implements GrafanaPromQLDatasourcePluginApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaPromQLDatasourcePluginApiImpl.class);

    protected HistorianService service;
    private int maxDataPointsAllowed;

    public GrafanaPromQLDatasourcePluginApiImpl(HistorianService historianService, int maxDataPointsAllowed) {
        this.service = historianService;
        this.maxDataPointsAllowed = maxDataPointsAllowed;
    }


    @Override
    public void root(RoutingContext context) {
        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end("Historian PromQL api is working fine, enjoy!");
    }

    /**
     * The /rules API endpoint returns a list of alerting and recording rules that are currently loaded.
     * In addition it returns the currently active alerts fired by the Prometheus instance of each alerting rule.
     *
     *
     * <pre>
     *     GET /api/v1/rules
     * </pre>
     * <p>
     * URL query parameters: - type=alert|record: return only the alerting rules (e.g. type=alert) or
     * the recording rules (e.g. type=record). When the parameter is absent or empty, no filtering is done.
     *
     *
     * <pre>
     * $ curl http://localhost:9090/api/v1/rules
     *
     * {
     *     "data": {
     *         "groups": [
     *             {
     *                 "rules": [
     *                     {
     *                         "alerts": [
     *                             {
     *                                 "activeAt": "2018-07-04T20:27:12.60602144+02:00",
     *                                 "annotations": {
     *                                     "summary": "High request latency"
     *                                 },
     *                                 "labels": {
     *                                     "alertname": "HighRequestLatency",
     *                                     "severity": "page"
     *                                 },
     *                                 "state": "firing",
     *                                 "value": "1e+00"
     *                             }
     *                         ],
     *                         "annotations": {
     *                             "summary": "High request latency"
     *                         },
     *                         "duration": 600,
     *                         "health": "ok",
     *                         "labels": {
     *                             "severity": "page"
     *                         },
     *                         "name": "HighRequestLatency",
     *                         "query": "job:request_latency_seconds:mean5m{job=\"myjob\"} > 0.5",
     *                         "type": "alerting"
     *                     },
     *                     {
     *                         "health": "ok",
     *                         "name": "job:http_inprogress_requests:sum",
     *                         "query": "sum by (job) (http_inprogress_requests)",
     *                         "type": "recording"
     *                     }
     *                 ],
     *                 "file": "/rules.yaml",
     *                 "interval": 60,
     *                 "name": "example"
     *             }
     *         ]
     *     },
     *     "status": "success"
     * }
     * </pre>
     *
     * @param context
     */
    @Override
    public void rules(RoutingContext context) {

        JsonObject response = new JsonObject()
                .put(STATUS, SUCCESS)
                .put(DATA, "");

        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(response.encode());
    }

    /**
     * The following endpoint evaluates an instant query at a single point in time:
     *
     * <pre>
     *     GET /api/v1/query
     *     POST /api/v1/query
     * </pre>
     * <p>
     * URL query parameters:
     *
     * <pre>
     *     query=<string>: Prometheus expression query string.
     *     time=<rfc3339 | unix_timestamp>: Evaluation timestamp. Optional.
     *     timeout=<duration>: Evaluation timeout. Optional. Defaults to and is capped by the value of the -query.timeout flag.     *
     * </pre>
     * <p>
     * The current server time is used if the time parameter is omitted.
     *
     * @param context The data section of the query result has the following format:
     *
     *                <pre>
     *                                                                                           {
     *                                                                                             "resultType": "matrix" | "vector" | "scalar" | "string",
     *                                                                                             "result": <value>
     *                                                                                           }
     *                                                                                           </pre>
     *
     *                <value> refers to the query result data, which has varying formats depending on the resultType. See the expression query result formats.
     *                <p>
     *                The following example evaluates the expression up at the time 2015-07-01T20:10:51.781Z:
     *
     *                <pre>
     *                                                                                           $ curl 'http://localhost:9090/api/v1/query?query=up&time=2015-07-01T20:10:51.781Z'
     *                                                                                           {
     *                                                                                              "status" : "success",
     *                                                                                              "data" : {
     *                                                                                                 "resultType" : "vector",
     *                                                                                                 "result" : [
     *                                                                                                    {
     *                                                                                                       "metric" : {
     *                                                                                                          "__name__" : "up",
     *                                                                                                          "job" : "prometheus",
     *                                                                                                          "instance" : "localhost:9090"
     *                                                                                                       },
     *                                                                                                       "value": [ 1435781451.781, "1" ]
     *                                                                                                    },
     *                                                                                                    {
     *                                                                                                       "metric" : {
     *                                                                                                          "__name__" : "up",
     *                                                                                                          "job" : "node",
     *                                                                                                          "instance" : "localhost:9100"
     *                                                                                                       },
     *                                                                                                       "value" : [ 1435781451.781, "0" ]
     *                                                                                                    }
     *                                                                                                 ]
     *                                                                                              }
     *                                                                                           }
     *                                                                                           </pre>
     */
    @Override
    public void query(RoutingContext context) {
        QueryRequest queryRequest = QueryRequest.builder()
                .parameters(getParameters(context))
                .build();


        // submit the request to the handler
        JsonObject response = new JsonObject()
                .put(STATUS, SUCCESS)
                .put(DATA, new JsonObject()
                        .put("resultType", "vector")
                        .put("result", new JsonArray()
                                .add(new JsonObject()
                                        .put("metric", new JsonObject()
                                                .put("__name__", "U004_TC01")
                                                .put("type", "temperature")
                                                .put("sub_unit", "reacteur1_coquille1")
                                                .put("measure", "pour_cent_op"))
                                        .put("value", new JsonArray()
                                                .add(queryRequest.getTime())
                                                .add("0.0000798")
                                        )
                                )
                                .add(new JsonObject()
                                        .put("metric", new JsonObject()
                                                .put("__name__", "U004_TC01")
                                                .put("type", "temperature")
                                                .put("sub_unit", "reacteur1_coquille1")
                                                .put("measure", "mesure_pv"))
                                        .put("value", new JsonArray()
                                                .add(queryRequest.getTime())
                                                .add("0.0001298")
                                        )
                                )
                        )
                );

        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(response.encode());


      /*  service.rxGetMetricsName(queryRequest.getMetricsParam())
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .errorMsg("Unexpected error :")
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(metricResponse -> {
                    JsonArray array = metricResponse.getJsonArray(HistorianServiceFields.METRICS);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(array.encode());
                }).subscribe();*/
    }

    private Map<String, String> getParameters(RoutingContext context) {
        Map<String, String> parameters = context.queryParams()
                .entries()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return parameters;
    }


    /**
     * used to get values of certain field
     *
     * @param context Expected request exemple :
     *                <pre>
     *                                                                                                {
     *                                                                                                   "field": "name",
     *                                                                                                   "query": "met",
     *                                                                                                   "limit": 100
     *                                                                                                }
     *                                                                                           </pre>
     *                "query" is optional.
     *                "limit" is optional.
     *                <p>
     *                response Example :
     *                <pre>
     *                                                                                               ["metric_25","metric_50","metric_75","metric_90","metric_95"]
     *                                                                                           </pre>
     */
    @Override
    public void labels(RoutingContext context) {
        LabelsRequest request = LabelsRequest.builder()
                .parameters(getParameters(context))
                .build();


        // submit the request to the handler
        JsonObject response = new JsonObject()
                .put(STATUS, SUCCESS)
                .put(DATA, new JsonArray().add("U004_TC01").add("U004_TC02"));

        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(response.encode());
/*
        service.rxGetFieldValues(request.getJson())
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .errorMsg("Unexpected error :")
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(valuesResponse -> {
                    JsonArray array = valuesResponse.getJsonArray(HistorianServiceFields.RESPONSE_VALUES);
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(array.encode());
                }).subscribe();*/
    }


    @Override
    public void metadata(RoutingContext context) {


        JsonObject response = new JsonObject()
                .put(STATUS, SUCCESS)
                .put(DATA, "");

        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(response.encode());
/*
        service.rxGetTagNames()
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .errorMsg("Unexpected error :")
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(tagsList -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(tagsList.encode());
                }).subscribe();*/
    }


    /**
     * used to query metrics datapoints in grafana panels.
     *
     * @param context Expected request exemple :
     *                <pre>
     *                                                                                             {
     *                                                                                               "from": "2016-10-31T06:33:44.866Z",
     *                                                                                               "to": "2020-10-31T12:33:44.866Z",
     *                                                                                               "names": ["metric_1"],
     *                                                                                               "format": "json",
     *                                                                                               "max_data_points": 8,
     *                                                                                               "tags": {
     *                                                                                                   "sensor" : "sensor_1"
     *                                                                                               },
     *                                                                                               "sampling":{
     *                                                                                                  "algorithm": "MIN",
     *                                                                                                  "bucket_size" : 100
     *                                                                                               }
     *                                                                                             }
     *                                                                                           </pre>
     *                response Exemple :
     *                <pre>
     *                                                                                           [
     *                                                                                             {
     *                                                                                               "target":"upper_75",
     *                                                                                               "tags" : {
     *                                                                                                   "sensor" : "sensor_1"
     *                                                                                               },
     *                                                                                               "datapoints":[
     *                                                                                                 [622,1450754160000],
     *                                                                                                 [365,1450754220000]
     *                                                                                               ]
     *                                                                                             },
     *                                                                                             {
     *                                                                                               "target":"upper_90",
     *                                                                                               "tags" : {
     *                                                                                                  "sensor" : "sensor_1"
     *                                                                                               },
     *                                                                                               "datapoints":[
     *                                                                                                 [861,1450754160000],
     *                                                                                                 [767,1450754220000]
     *                                                                                               ]
     *                                                                                             }
     *                                                                                           ]
     *                                                                                           </pre>
     *                <p>
     *                le champs "tags" n'est retourné que si présent dans la requête.
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     * https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     * </a>
     */
    @Override
    public void queryRange(RoutingContext context) {
        final long startRequest = System.currentTimeMillis();
        final QueryRequest request = QueryRequest.builder()
                .parameters(getParameters(context))
                .build();


        // submit the request to the handler
        JsonObject response = new JsonObject()
                .put(STATUS, SUCCESS)
                .put(DATA, new JsonObject()
                        .put("resultType", "matrix")
                        .put("result", new JsonArray()
                                .add(new JsonObject()
                                        .put("metric", new JsonObject()
                                                .put("__name__", "U004_TC01")
                                                .put("type", "temperature")
                                                .put("sub_unit", "reacteur1_coquille1")
                                                .put("measure", "pour_cent_op"))
                                        .put("values", new JsonArray()
                                                .add(new JsonArray()
                                                        .add(request.getStart())
                                                        .add("0.0000798")
                                                )
                                                .add(new JsonArray()
                                                        .add(request.getStart() + 100 )
                                                        .add("0.0001298")
                                                )
                                                .add(new JsonArray()
                                                        .add(request.getEnd())
                                                        .add("0.0001798")
                                                )
                                        )
                                )
                                .add(new JsonObject()
                                        .put("metric", new JsonObject()
                                                .put("__name__", "U004_TC01")
                                                .put("type", "temperature")
                                                .put("sub_unit", "reacteur1_coquille1")
                                                .put("measure", "mesure_pv"))
                                        .put("values", new JsonArray()
                                                .add(new JsonArray()
                                                        .add(request.getStart())
                                                        .add("0.0001798")
                                                )
                                                .add(new JsonArray()
                                                        .add(request.getStart() + 100 )
                                                        .add("0.0002298")
                                                )
                                                .add(new JsonArray()
                                                        .add(request.getEnd())
                                                        .add("0.0003798")
                                                )
                                        )
                                )
                        )
                );

        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(response.encode());


       /* final JsonObject getTimeSeriesChunkParams = buildGetTimeSeriesRequest(request);
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
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .errorMsg("Unexpected error :")
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(timeseries -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(timeseries.encode());
                    LOGGER.debug("body :: {}", timeseries);
                }).subscribe();*/
    }


    /**
     * used to the find annotations.
     *
     * @param context Expected request exemple :
     *                <pre>
     *                                                                                           {
     *                                                                                               "from": "2020-2-14T01:43:14.070Z",
     *                                                                                               "to": "2020-2-14T06:50:14.070Z",
     *                                                                                               "limit" : 100,
     *                                                                                               "tags": ["tag1", "tag2"],
     *                                                                                               "matchAny": false,
     *                                                                                               "type": "tags"
     *                                                                                           }
     *                                                                                           </pre>
     *                response Exemple :
     *                <pre>
     *                                                                                           {
     *                                                                                             "annotations" : [
     *                                                                                               {
     *                                                                                                 "time": 1581648194070,
     *                                                                                                 "text": "annotation 1",
     *                                                                                                 "tags": ["tag1","tag2"]
     *                                                                                               }
     *                                                                                             ],
     *                                                                                             "total_hit" : 1
     *                                                                                           }
     *                                                                                           </pre>
     * @see <a href="https://grafana.com/grafana/plugins/grafana-simple-json-datasource.">
     * https://grafana.com/grafana/plugins/grafana-simple-json-datasource.
     * </a>
     */
    @Override
    public void series(RoutingContext context) {

        final QueryRequest request = QueryRequest.builder()
                .parameters(getParameters(context))
                .build();


        JsonObject response = new JsonObject()
                .put(STATUS, SUCCESS)
                .put(DATA, new JsonArray()
                        .add(new JsonObject().put("__name__", "U004_TC01").put("type", "temperature").put("sub_unit", "reacteur1_coquille1").put("measure", "pour_cent_op"))
                        .add(new JsonObject().put("__name__", "U004_TC01").put("type", "temperature").put("sub_unit", "reacteur1_coquille1").put("measure", "mesure_pv"))
                        .add(new JsonObject().put("__name__", "U004_TC01").put("type", "temperature").put("sub_unit", "reacteur1_coquille1").put("measure", "consigne_sp"))
                        .add(new JsonObject().put("__name__", "U004_TC02").put("type", "temperature").put("sub_unit", "reacteur1_coquille2").put("measure", "pour_cent_op"))
                        .add(new JsonObject().put("__name__", "U004_TC02").put("type", "temperature").put("sub_unit", "reacteur1_coquille2").put("measure", "mesure_pv"))
                );

        context.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(response.encode());


        /*final AnnotationRequestParam request;
        try {
            JsonObject requestBody = context.getBodyAsJson();
            request = new HurenceDatasourcePluginAnnotationRequestParser(
                    FROM_JSON_PATH, TO_JSON_PATH, TAGS_JSON_PATH, TYPE_JSON_PATH, LIMIT_JSON_PATH, MATCH_ANY_JSON_PATH
            ).parseRequest(requestBody);
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

        final JsonObject getAnnotationParams = buildHistorianAnnotationRequest(request);

        service
                .rxGetAnnotations(getAnnotationParams)
                .doOnError(ex -> {
                    LOGGER.error("Unexpected error : ", ex);
                    VertxErrorAnswerDescription error = VertxErrorAnswerDescription.builder()
                            .errorMsg("Unexpected error :")
                            .throwable(ex)
                            .routingContext(context)
                            .build();
                    VertxHttpErrorMsgHelper.answerWithError(error);
                })
                .doOnSuccess(annotations -> {
                    context.response().setStatusCode(200);
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().end(annotations.encode());
                }).subscribe();*/
    }


}
