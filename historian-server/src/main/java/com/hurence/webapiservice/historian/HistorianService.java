package com.hurence.webapiservice.historian;

import com.hurence.historian.model.solr.SolrFieldMapping;
import com.hurence.historian.model.HistorianServiceFields;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


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


    /**
     * @param myParams        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#FROM} : start date,
     *                          {@value HistorianServiceFields#TO} : end date,
     *                          {@value HistorianServiceFields#TAGS} : as key value,
     *                          {@value HistorianServiceFields#NAMES} : list of metric to query with optionally more info,
     *                          {@value HistorianServiceFields#SAMPLING_ALGO} : algorithm name to use,
     *                          {@value HistorianServiceFields#BUCKET_SIZE} : buvket size to use for sampling,
     *                          {@value HistorianServiceFields#MAX_POINT_BY_METRIC} : maximum number of point desired
     *                      }
     *                      </pre>
     *                      explanation :
     *                      [Optional] if {@value HistorianServiceFields#FROM} not specified will search from 0
     *                      [Optional] if {@value HistorianServiceFields#TO} not specified will search to Max.Long*
     *                      [Optional] use {@value HistorianServiceFields#TAGS} to search for specific timeseries having one of those tags
     *                      [Optional] use {@value HistorianServiceFields#NAMES} to search a specific timeseries name
     *                      [Required] use {@value HistorianServiceFields#SAMPLING_ALGO} is the algorithm to use if sampling is needed
     *                      [Required] use {@value HistorianServiceFields#BUCKET_SIZE} is the bucket size to use if sampling is needed
     *                      [Required] use {@value HistorianServiceFields#MAX_POINT_BY_METRIC} is the max number of point to return by metric name
     *
     *        {@value HistorianServiceFields#NAMES} must be an array each element must be either a string either an object.
     *        - When this is just a string, this should correspond to the metric name wanted. In this case
     *          we will use tags an sampling options specified in root oject to query the metric.
     *        - When this is just an object, it should contain at least a field {@value HistorianServiceFields#NAMES}
     *        corresponding to the metric name wanted. And it can also contains tags ans sampling options to use for this
     *                        specific metric. Here an exemple containing all available options:
     *          <pre>
     *          {
     *              {@value SolrFieldMapping#CHUNK_NAME} : the metric name,
     *              {@value HistorianServiceFields#TAGS} : the tags asked for this metric
     *          }
     *          </pre>
     *
     *
     * @param myResult return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#TOTAL_POINTS} : "the total number of point returned (after sampling)",
     *                          {@value HistorianServiceFields#TIMESERIES} : [
     *                              {
     *                                  {@value SolrFieldMapping#CHUNK_NAME} : "the metric name",
     *                                  {@value HistorianServiceFields#TOTAL_POINTS} : "the total number of point returned for this metric (after sampling)",
     *                                  {@value HistorianServiceFields#TAGS} : {
     *                                      "tag name 1" : "tag value",
     *                                      ...
     *                                  },
     *                                  {@value HistorianServiceFields#DATAPOINTS} : [
     *                                      [value(double), timestamp(long)],
     *                                      ...
     *                                  ],
     *                                  {@value HistorianServiceFields#AGGREGATION} : [
     *                                      aggName : aggvalue,
     *                                      ...
     *                                  ]
     *                              }
     *                          ]
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getTimeSeries(JsonObject myParams, Handler<AsyncResult<JsonObject>> myResult);

    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#FROM} : "content of chunks as an array",
     *                          {@value HistorianServiceFields#TO} : "total chunk matching query",
     *                          {@value HistorianServiceFields#FIELDS} : ["field1", "field2"...],
     *                          {@value HistorianServiceFields#TAGS} : "total chunk matching query",
     *                          {@value HistorianServiceFields#NAMES} : "content of chunks as an array",
     *                      }
     *                      </pre>
     *                      explanation :
     *                      if {@value HistorianServiceFields#FROM} not specified will search from 0
     *                      if {@value HistorianServiceFields#TO} not specified will search to Max.Long
     *                      use {@value HistorianServiceFields#FIELDS} if you want to retrieve some of the precalculated aggs. If not specified retrieve all.
     *                      use {@value HistorianServiceFields#TAGS} to search for specific timeseries having one of those tags
     *                      use {@value HistorianServiceFields#NAMES} to search a specific timeseries name
     * @param resultHandler return chunks of timeseries as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#CHUNKS} : "content of chunks as an array",
     *                          {@value HistorianServiceFields#TOTAL} : "total chunk matching query",
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
     *                          {@value HistorianServiceFields#METRIC} : "A string to help finding desired metric",
     *                          {@value HistorianServiceFields#LIMIT} : <maximum number of metric to return>(int)
     *                      }
     *                      </pre>
     * @param resultHandler return names of metrics as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#METRICS} : "all metric name matching the query",
     *                          {@value HistorianServiceFields#TOTAL} : <Number of metric returned>(int)
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
     *                          {@value HistorianServiceFields#FIELD} : "A string of the field to search for it's values",
     *                          {@value HistorianServiceFields#QUERY} : "a query to use in searching the values",
     *                          {@value HistorianServiceFields#LIMIT} : <maximum number of metric to return>(int)
     *                      }
     *                      </pre>
     * @param resultHandler return names of metrics as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#RESPONSE_VALUES} : "all field values matching the query",
     *                          {@value HistorianServiceFields#TOTAL} : <Number of metric returned>(int)
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getFieldValues(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    @Fluent
    HistorianService getTagNames(Handler<AsyncResult<JsonArray>> resultHandler);

    /**
     * @param params        as a json object
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#FROM} : "start of the date range",
     *                          {@value HistorianServiceFields#TO} : "end of the date range",
     *                          {@value HistorianServiceFields#TYPE} : either "all" either "tags",
     *                          {@value HistorianServiceFields#TAGS} : if the request "type" is "tags" this is used to filter annotation by tags otherwise it is not used.,
     *                          {@value HistorianServiceFields#LIMIT} : the max number of annotation to return,
     *                          {@value HistorianServiceFields#MATCH_ANY} : if true, we should return any annotation containing at leas one of the tags. If false we should return only annotation containing all the tags,
     *                      }
     *                      </pre>
     * @param resultHandler return annotations as an array of
     *                      <pre>
     *                      {
     *                          {@value HistorianServiceFields#ANNOTATIONS} : "all annotation matching the query",
     *                          {@value HistorianServiceFields#TOTAL} : "total annotations matching query"
     *                      }
     *                      </pre>
     * @return himself
     */
    @Fluent
    HistorianService getAnnotations(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * @param timeseries        as a json object
     *                          <pre>
     *                          {    "correctPoints" : [
     *                                  {
     *                                      {@value SolrFieldMapping#CHUNK_NAME} : "metric name to add datapoints",
     *                                      {@value HistorianServiceFields#POINTS } : [
     *                                          [timestamp, value, quality]
     *                                          ...
     *                                          [timestamp, value, quality]
     *                                      ]
     *                                  }
     *                                  {
     *                                      {@value SolrFieldMapping#CHUNK_NAME} : "metric name to add datapoints",
     *                                      {@value HistorianServiceFields#POINTS } : [
     *                                          [timestamp, value, quality]
     *                                          ...
     *                                          [timestamp, value, quality]
     *                                      ]
     *                                  }
     *                                   ...
     *                              ]
     *                              {@value HistorianServiceFields#GROUPED_BY} : [groupedByField_1, groupedByField_2, ...]
     *                      }
     *                          </pre>
     *                               The quality is optional but should either be present for all datapoints or 0.
     * @param resultHandler
     * @return himself
     */
    @Fluent
    HistorianService addTimeSeries(JsonObject timeseries, Handler<AsyncResult<JsonObject>> resultHandler);
}
