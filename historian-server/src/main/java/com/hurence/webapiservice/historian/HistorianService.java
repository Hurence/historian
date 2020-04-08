package com.hurence.webapiservice.historian;

import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
import com.hurence.webapiservice.historian.impl.SolrHistorianServiceImpl;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import com.hurence.historian.modele.HistorianFields;


/**
 * This interface describes the Transactions Manager Service. Note that all methods has same name of corresponding operation id
 */
@ProxyGen
@VertxGen
public interface HistorianService {


    @GenIgnore
    static HistorianService create(Vertx vertx, SolrHistorianConf historianConf,
                                   Handler<AsyncResult<HistorianService>> readyHandler) {
        return new SolrHistorianServiceImpl(vertx, historianConf, readyHandler);
    }

    @GenIgnore
    static com.hurence.webapiservice.historian.reactivex.HistorianService createProxy(Vertx vertx, String address) {
        return new com.hurence.webapiservice.historian.reactivex.HistorianService(
                new HistorianServiceVertxEBProxy(vertx, address)
        );
    }


    @Fluent
    HistorianService getTimeSeries(JsonObject myParams, Handler<AsyncResult<JsonObject>> myResult);

    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#FROM_REQUEST_FIELD} : "content of chunks as an array",
     *                          {@value HistorianFields#TO_REQUEST_FIELD} : "total chunk matching query",
     *                          {@value HistorianFields#FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD} : ["field1", "field2"...],
     *                          {@value HistorianFields#TAGS_TO_FILTER_ON_REQUEST_FIELD} : "total chunk matching query",
     *                          {@value HistorianFields#METRIC_NAMES_AS_LIST_REQUEST_FIELD} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value HistorianFields#FROM_REQUEST_FIELD} not specified will search from 0
     *                      if {@value HistorianFields#TO_REQUEST_FIELD} not specified will search to Max.Long
     *                      use {@value HistorianFields#FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value HistorianFields#TAGS_TO_FILTER_ON_REQUEST_FIELD} to search for specific timeseries having one of those tags
     *                      use {@value HistorianFields#METRIC_NAMES_AS_LIST_REQUEST_FIELD} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#RESPONSE_CHUNKS} : "content of chunks as an array",
     *                          {@value HistorianFields#RESPONSE_TOTAL_FOUND} : "total chunk matching query",
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);


    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#FROM_REQUEST_FIELD} : "content of chunks as an array",
     *                          {@value HistorianFields#TO_REQUEST_FIELD} : "total chunk matching query",
     *                          {@value HistorianFields#FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD} : ["field1", "field2"...],
     *                          {@value HistorianFields#TAGS_TO_FILTER_ON_REQUEST_FIELD} : "total chunk matching query",
     *                          {@value HistorianFields#METRIC_NAMES_AS_LIST_REQUEST_FIELD} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value HistorianFields#FROM_REQUEST_FIELD} not specified will search from 0
     *                      if {@value HistorianFields#TO_REQUEST_FIELD} not specified will search to Max.Long
     *                      use {@value HistorianFields#FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value HistorianFields#TAGS_TO_FILTER_ON_REQUEST_FIELD} to search for specific timeseries having one of those tags
     *                      use {@value HistorianFields#METRIC_NAMES_AS_LIST_REQUEST_FIELD} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#RESPONSE_CHUNKS} : "content of chunks as an array",
     *                          {@value HistorianFields#RESPONSE_TOTAL_FOUND} : "total chunk matching query",
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService compactTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);




    /**
     * @param params        as a json object, it is ignored at the moment TODO
     * @param resultHandler return names of metrics as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#RESPONSE_METRICS} : "all metric name matching the query",
     *                          {@value HistorianFields#RESPONSE_TOTAL_FOUND} : "total metric names matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#FROM_REQUEST_FIELD} : "start of the date range",
     *                          {@value HistorianFields#TO_REQUEST_FIELD} : "end of the date range",
     *                          {@value HistorianFields#TYPE_REQUEST_FIELD} : either "all" either "tags",
     *                          {@value HistorianFields#TAGS_TO_FILTER_ON_REQUEST_FIELD} : if the request "type" is "tags" this is used to filter annotation by tags otherwise it is not used.,
     *                          {@value HistorianFields#MAX_ANNOTATION_REQUEST_FIELD} : the max number of annotation to return,
     *                          {@value HistorianFields#MATCH_ANY_REQUEST_FIELD} : if true, we should return any annotation containing at leas one of the tags. If false we should return only annotation containing all the tags,
     *                      }
     *                      </pre>
     * @param resultHandler return annotations as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#RESPONSE_ANNOTATIONS} : "all annotation matching the query",
     *                          {@value HistorianFields#RESPONSE_TOTAL_FOUND} : "total annotations matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getAnnotations(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);


}