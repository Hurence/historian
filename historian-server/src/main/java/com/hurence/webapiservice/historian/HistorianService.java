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
import io.vertx.core.json.JsonArray;
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
     *                          {@value HistorianFields#FROM} : "content of chunks as an array",
     *                          {@value HistorianFields#TO} : "total chunk matching query",
     *                          {@value HistorianFields#FIELDS} : ["field1", "field2"...],
     *                          {@value HistorianFields#TAGS} : "total chunk matching query",
     *                          {@value HistorianFields#NAMES} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value HistorianFields#FROM} not specified will search from 0
     *                      if {@value HistorianFields#TO} not specified will search to Max.Long
     *                      use {@value HistorianFields#FIELDS} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value HistorianFields#TAGS} to search for specific timeseries having one of those tags
     *                      use {@value HistorianFields#NAMES} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#CHUNKS} : "content of chunks as an array",
     *                          {@value HistorianFields#TOTAL} : "total chunk matching query",
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
     *                          {@value HistorianFields#FROM} : "content of chunks as an array",
     *                          {@value HistorianFields#TO} : "total chunk matching query",
     *                          {@value HistorianFields#FIELDS} : ["field1", "field2"...],
     *                          {@value HistorianFields#TAGS} : "total chunk matching query",
     *                          {@value HistorianFields#NAMES} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value HistorianFields#FROM} not specified will search from 0
     *                      if {@value HistorianFields#TO} not specified will search to Max.Long
     *                      use {@value HistorianFields#FIELDS} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value HistorianFields#TAGS} to search for specific timeseries having one of those tags
     *                      use {@value HistorianFields#NAMES} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#CHUNKS} : "content of chunks as an array",
     *                          {@value HistorianFields#TOTAL} : "total chunk matching query",
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService compactTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);




    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#METRIC} : "A string to help finding desired metric",
     *                          {@value HistorianFields#LIMIT} : <maximum number of metric to return>(int)
     *                      }
     *                      </pre>
     * @param resultHandler return names of metrics as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#METRICS} : "all metric name matching the query",
     *                          {@value HistorianFields#TOTAL} : <Number of metric returned>(int)
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
     *                          {@value HistorianFields#FROM} : "start of the date range",
     *                          {@value HistorianFields#TO} : "end of the date range",
     *                          {@value HistorianFields#TYPE} : either "all" either "tags",
     *                          {@value HistorianFields#TAGS} : if the request "type" is "tags" this is used to filter annotation by tags otherwise it is not used.,
     *                          {@value HistorianFields#LIMIT} : the max number of annotation to return,
     *                          {@value HistorianFields#MATCH_ANY} : if true, we should return any annotation containing at leas one of the tags. If false we should return only annotation containing all the tags,
     *                      }
     *                      </pre>
     * @param resultHandler return annotations as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianFields#ANNOTATIONS} : "all annotation matching the query",
     *                          {@value HistorianFields#TOTAL} : "total annotations matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getAnnotations(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * @param timeseries        as a json object
     *                          <pre>
     *                          [
     *                              {
     *                                  {@value HistorianFields#NAME} : "metric name to add datapoints",
     *                                  {@value HistorianFields#POINTS_REQUEST_FIELD } : [
     *                                      [timestamp, value, quality]
     *                                      ...
     *                                      [timestamp, value, quality]
     *                                  ]
     *                              }
     *                          ]
     *                          </pre>
     *                               The quality is optional but should either be present for all datapoints or 0.
     * @param resultHandler
     * @return himself
     */
    @Fluent
    HistorianService addTimeSeries(JsonArray timeseries, Handler<AsyncResult<JsonObject>> resultHandler);
}
