package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianConf;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.historian.modele.solr.SolrFieldMapping;
import com.hurence.historian.modele.stream.ChunkStream;
import com.hurence.timeseries.modele.chunk.Chunk;
import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.models.MetricSizeInfo;
import com.hurence.webapiservice.historian.models.MetricsSizeInfo;
import com.hurence.webapiservice.historian.models.MetricsSizeInfoImpl;
import com.hurence.webapiservice.historian.SolrHistorianConf;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.*;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class GetTimeSeriesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTimeSeriesHandler.class);
    HistorianConf historianConf;
    SolrHistorianConf solrHistorianConf;

    public GetTimeSeriesHandler(HistorianConf historianConf, SolrHistorianConf solrHistorianConf) {
        this.historianConf = historianConf;
        this.solrHistorianConf = solrHistorianConf;
    }


    /**
     * nombre point < LIMIT_TO_DEFINE ==> Extract points from chunk
     * nombre point >= LIMIT_TO_DEFINE && nombre de chunk < LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg)
     * nombre de chunk >= LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg),
     * but should using agg on solr side (using key partition, by month, daily ? yearly ?)
     */
    public Handler<Promise<JsonObject>> getTimeSeriesHandler(JsonObject myParams) {
        Request request = new Request(myParams);
        return getTimeSeriesHandler(request);
    }

    private Handler<Promise<JsonObject>> getTimeSeriesHandler(Request request) {
        List<AGG> aggregationList = request.getAggs();
        final SolrQuery query = buildTimeSeriesQuery(request);
        LOGGER.debug("solrQuery : {}", query.toQueryString());
        return p -> {
            MetricsSizeInfo metricsInfo;
            try {
                metricsInfo = getNumberOfPointsByMetricInRequest(request.getMetricRequestsWithFinalTags(), query);
                LOGGER.debug("metrics info to query : {}", metricsInfo);
                if (metricsInfo.isEmpty()) {
                    final MultiTimeSeriesExtracter timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(request, metricsInfo, aggregationList);
                    p.complete(buildTimeSeriesResponse(timeSeriesExtracter));
                    return;
                }
                final MultiTimeSeriesExtracter timeSeriesExtracter = getMultiTimeSeriesExtracter(request, query, metricsInfo, aggregationList);
                requestSolrAndBuildTimeSeries(query, p, timeSeriesExtracter);
            } catch (IOException e) {
                LOGGER.error("unexpected io exception", e);
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
    }


    private SolrQuery buildTimeSeriesQuery(Request request) {
        StringBuilder queryBuilder = new StringBuilder();
        if (request.getTo() != null) {
            LOGGER.trace("requesting timeseries to {}", request.getTo());
            queryBuilder.append(getHistorianFields().CHUNK_START_FIELD).append(":[* TO ").append(request.getTo()).append("]");
        }
        if (request.getFrom()  != null) {
            LOGGER.trace("requesting timeseries from {}", request.getFrom());
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(getHistorianFields().CHUNK_END_FIELD).append(":[").append(request.getFrom()).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());

        //FILTER
        buildFilters(request, query);
        //    FIELDS_TO_FETCH
        query.setFields(getHistorianFields().CHUNK_START_FIELD,
                getHistorianFields().CHUNK_END_FIELD,
                getHistorianFields().CHUNK_COUNT_FIELD,
                getHistorianFields().CHUNK_NAME);
        addAllTagsAsFields(request.getMetricRequestsWithFinalTags(), query);
        addFieldsThatWillBeNeededByAggregations(request.getAggs(), query);
        //    SORT
        query.setSort(getHistorianFields().CHUNK_START_FIELD, SolrQuery.ORDER.asc);
        query.addSort(getHistorianFields().CHUNK_END_FIELD, SolrQuery.ORDER.asc);
        query.setRows(request.getMaxTotalChunkToRetrieve());

        return query;
    }

    private SolrFieldMapping getHistorianFields() {
        return this.historianConf.getFieldsInSolr();
    }

    private void addAllTagsAsFields(List<MetricRequest> metricRequests, SolrQuery query) {
        Set<String> tags = new HashSet<>();
        metricRequests.forEach(metricRequest -> tags.addAll(metricRequest.getTags().keySet()));
        tags.forEach(query::addField);
    }

    private void buildFilters(Request request, SolrQuery query) {
        List<MetricRequest> metricOptions = request.getMetricRequestsWithFinalTags();
        List<String> metricFilters = buildFilterForEachMetric(metricOptions);
        String finalFilter = buildFinalFilterQuery(metricFilters);
        query.addFilterQuery(finalFilter);
    }

    /**
     *        Joins every metric filter by a "OR"
     * @param metricFilters
     * @return final filter query should be
     * <pre>
     *     (filter1) OR (filter2) OR ... OR (filtern)
     * </pre>
     *
     */
    private String buildFinalFilterQuery(List<String> metricFilters) {
        return metricFilters.stream().collect(Collectors.joining(") OR (", "(", ")"));
    }

    /**
     * filter query for each metric
     *     each string should looks like "(name:A && usine:usine_1 && sensor:sensor_1)"
     *     we filter on metric name and on every tags we get. we use tags of metricOptions and of rootTags.
     *     If there is conflict then tags of metricOptions have priority.
     * @param metricOptions
     * @return filter query for each metric
     *         each string should looks like :
     *         <pre>
     *             name:"A" AND usine:"usine_1" AND sensor:"sensor_1"
     *         </pre>
     *
     *
     */
    private List<String> buildFilterForEachMetric(List<MetricRequest> metricOptions) {
        List<String> finalStringList = new ArrayList<>();
        metricOptions.forEach(metricRequest -> {
            Map<String,String> finalTagsForMetric = new HashMap<>();
            metricRequest.getTags().forEach(finalTagsForMetric::put);
            List<String> tagsFilter = new ArrayList<>();
            finalTagsForMetric.forEach((key, value) -> {
                tagsFilter.add(key+":\""+value+"\"");
            });
            if(!tagsFilter.isEmpty())
                finalStringList.add(tagsFilter.stream().collect(Collectors.joining(" AND ", getHistorianFields().CHUNK_NAME +":\""+metricRequest.getName()+"\" AND ", "")));
            else
                finalStringList.add(getHistorianFields().CHUNK_NAME + ":\""+metricRequest.getName()+"\"");
        });
        return finalStringList;
    }

    private void addFieldsThatWillBeNeededByAggregations(List<AGG> aggregationList, SolrQuery query) {
        aggregationList.forEach(agg -> {
            switch (agg) {
                case AVG:
                    query.addField(getHistorianFields().CHUNK_AVG_FIELD);
                    break;
                case SUM:
                    query.addField(getHistorianFields().CHUNK_SUM_FIELD);
                    break;
                case MIN:
                    query.addField(getHistorianFields().CHUNK_MIN_FIELD);
                    break;
                case MAX:
                    query.addField(getHistorianFields().CHUNK_MAX_FIELD);
                    break;
                case COUNT:
                    query.addField(getHistorianFields().CHUNK_COUNT_FIELD);
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
    }

    private MetricsSizeInfo getNumberOfPointsByMetricInRequest(List<MetricRequest> requests, SolrQuery query) throws IOException {
        StringBuilder exprBuilder = new StringBuilder("rollup(search(").append(solrHistorianConf.chunkCollection)
                .append(",q=").append(query.getQuery());
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append(",fq=").append(filterQuery);
            }
        }

        List<String> neededFields = findNeededTagsName(requests);
        neededFields.add(getHistorianFields().CHUNK_NAME);
        List<String> overFields = new ArrayList<>(neededFields);
        String overString = joinListAsString(overFields);
        neededFields.add(getHistorianFields().CHUNK_COUNT_FIELD);
        String flString = joinListAsString(neededFields);
        exprBuilder.append(",fl=\"").append(flString).append("\"")
                .append(",qt=\"/export\", sort=\"").append(getHistorianFields().CHUNK_NAME).append(" asc\")")
                .append(",over=\"").append(overString).append("\"")
                .append(", sum(").append(getHistorianFields().CHUNK_COUNT_FIELD).append("), count(*))");
        LOGGER.trace("expression is : {}", exprBuilder.toString());
        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", exprBuilder.toString());
        paramsLoc.set("qt", "/stream");
        TupleStream solrStream = new SolrStream(solrHistorianConf.streamEndPoint, paramsLoc);
        StreamContext context = new StreamContext();
        solrStream.setStreamContext(context);
        solrStream.open();
        Tuple tuple = solrStream.read();
        MetricsSizeInfoImpl metricsInfo = new MetricsSizeInfoImpl();
        while (!tuple.EOF) {
            LOGGER.trace("tuple : {}", tuple.jsonStr());//TODO truncate chunk if necessary how ?
            for (MetricRequest request: requests) {
                if (isTupleMatchingMetricRequest(request, tuple)) {
                    metricsInfo.increaseNumberOfChunksForMetricRequest(request,
                            tuple.getLong("count(*)")
                    );
                    metricsInfo.increaseNumberOfPointsForMetricRequest(request,
                            tuple.getLong("sum(" + getHistorianFields().CHUNK_COUNT_FIELD + ")")
                    );
                }
            }
            tuple = solrStream.read();
        }
        LOGGER.debug("metric response : {}", tuple.jsonStr());
        solrStream.close();
        return metricsInfo;
    }

    /**
     * join string so that we got somthing like <pre>elem1,elem2,elem3</pre>
     * @param neededFields
     * @return
     */
    private String joinListAsString(List<String> neededFields) {
        return String.join(",", neededFields);
    }

    /**
     * return all tags name needed for querying metrics.
     * @param requests
     * @return
     */
    private List<String> findNeededTagsName(List<MetricRequest> requests) {
        Set<String> tagsSet = new HashSet<>();
        requests.forEach(metricRequest -> {
            tagsSet.addAll(metricRequest.getTags().keySet());
        });
        return new ArrayList<>(tagsSet);
    }

    /**
     * return true if the tuple (one document response from solr) match the query for MetricRequest
     *
     * @param request the asked query. This is a combinaison of "name" and tags.
     * @param tuple the count of chunks and points for all chunk matching the query.
     *              There is a tuple for each combinaison of name/tag found.
     *
     *              for example something like that (each line is a tuple except the header)
     *              <pre>
     *              name    | usine     | sensor    | count(*) | sum(chunk_count)
     *              temp_a  | usine_1   | sensor_1  | 100      | 1000
     *              temp_a  | usine_1   | sensor_2  | 10       | 100
     *              temp_b  | usine_1   | null      | 100      | 1000
     *              </pre>
     * @return
     */
    private boolean isTupleMatchingMetricRequest(MetricRequest request, Tuple tuple) {
        if (!request.getName().equals(tuple.fields.get(getHistorianFields().CHUNK_NAME))){
            return false;
        } else {
            for (Map.Entry<String, String> entry : request.getTags().entrySet()) {
                if (!tuple.fields.get(entry.getKey()).toString().equals(entry.getValue()))
                    return false;
            }
            return true;
        }

    }


    public MultiTimeSeriesExtracter getMultiTimeSeriesExtracter(Request request, SolrQuery query, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        //TODO make three different group for each metrics, not use a single strategy globally for all metrics.
        final MultiTimeSeriesExtracter timeSeriesExtracter;
        if (metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint ||
                metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(request).getMaxPoint() ||
                metricsInfo.getTotalNumberOfChunks() < getSamplingConf(request).getMaxPoint()
        ) {
            LOGGER.debug("QUERY MODE 1: metricsInfo.getTotalNumberOfPoints() < limitNumberOfPoint");
            query.addField(getHistorianFields().CHUNK_VALUE_FIELD);
            timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(request, metricsInfo, aggregationList);
        } else if (metricsInfo.getTotalNumberOfChunks() < solrHistorianConf.limitNumberOfChunks) {
            LOGGER.debug("QUERY MODE 2: metricsInfo.getTotalNumberOfChunks() < limitNumberOfChunks");
            addFieldsThatWillBeNeededBySamplingAlgorithms(request, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(request, metricsInfo, aggregationList);
        } else {
            LOGGER.debug("QUERY MODE 3 : else");
            //TODO Sample points with chunk aggs depending on alg (min, avg),
            // but should using agg on solr side (using key partition, by month, daily ? yearly ?)
            // For the moment we use the stream api without partitionning
            addFieldsThatWillBeNeededBySamplingAlgorithms(request, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(request, metricsInfo, aggregationList);
        }
        return timeSeriesExtracter;
    }


    public void requestSolrAndBuildTimeSeries(SolrQuery query, Promise<JsonObject> p, MultiTimeSeriesExtracter timeSeriesExtracter) {
        try (ChunkStream stream = queryStream(query)) {
            JsonObject timeseries = extractTimeSeriesThenBuildResponse(stream, timeSeriesExtracter);
            p.complete(timeseries);
        } catch (Exception e) {
            LOGGER.error("unexpected exception while reading JsonStream", e);
            p.fail(e);
        }
    }

    public void addFieldsThatWillBeNeededBySamplingAlgorithms(Request request, SolrQuery query, MetricsSizeInfo metricsInfo) {
        SamplingConf requestedSamplingConf = getSamplingConf(request);
        Set<SamplingAlgorithm> samplingAlgos = determineSamplingAlgoThatWillBeUsed(requestedSamplingConf, metricsInfo);
        addNecessaryFieldToQuery(query, samplingAlgos);
    }




    private void addNecessaryFieldToQuery(SolrQuery query, Set<SamplingAlgorithm> samplingAlgos) {
        samplingAlgos.forEach(algo -> {
            switch (algo) {
                case NONE:
                    query.addField(getHistorianFields().CHUNK_VALUE_FIELD);
                    break;
                case FIRST:
                    query.addField(getHistorianFields().CHUNK_FIRST_VALUE_FIELD);
                    break;
                case AVERAGE:
                    query.addField(getHistorianFields().CHUNK_SUM_FIELD);
                    break;
                case MIN:
                    query.addField(getHistorianFields().CHUNK_MIN_FIELD);
                    break;
                case MAX:
                    query.addField(getHistorianFields().CHUNK_MAX_FIELD);
                    break;
                case MODE_MEDIAN:
                case LTTB:
                case MIN_MAX:
                default:
                    throw new IllegalStateException("algorithm " + algo.name() + " is not yet supported !");
            }
        });
    }

    private Set<SamplingAlgorithm> determineSamplingAlgoThatWillBeUsed(SamplingConf askedSamplingConf, MetricsSizeInfo metricsSizeInfo) {
        if (askedSamplingConf.getAlgo() != SamplingAlgorithm.NONE) {
            Set<SamplingAlgorithm> algos = new HashSet<SamplingAlgorithm>();
            algos.add(askedSamplingConf.getAlgo());
            return algos;
        }
        return metricsSizeInfo.getMetricRequests().stream()
                .map(metricrequest -> {
                    MetricSizeInfo metricInfo = metricsSizeInfo.getMetricInfo(metricrequest);
                    SamplingAlgorithm algo = TimeSeriesExtracterUtil.calculSamplingAlgorithm(askedSamplingConf, metricInfo.totalNumberOfPoints);
                    return algo;
                }).collect(Collectors.toSet());
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtractorUsingPreAgg
    private MultiTimeSeriesExtracter createTimeSerieExtractorUsingChunks(Request request, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        long from = request.getFrom();
        long to = request.getTo();
        SamplingConf requestedSamplingConf = getSamplingConf(request);
        MultiTimeSeriesExtractorUsingPreAgg timeSeriesExtracter = new MultiTimeSeriesExtractorUsingPreAgg(from, to, requestedSamplingConf, request.getMetricRequestsWithFinalTags());
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtracterImpl
    private MultiTimeSeriesExtracter createTimeSerieExtractorSamplingAllPoints(Request request, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        long from = request.getFrom();
        long to = request.getTo();
        SamplingConf requestedSamplingConf = getSamplingConf(request);
        MultiTimeSeriesExtracterImpl timeSeriesExtracter = new MultiTimeSeriesExtracterImpl(from, to, requestedSamplingConf, request.getMetricRequestsWithFinalTags());
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    private void fillingExtractorWithAggregToReturn(MultiTimeSeriesExtracterImpl timeSeriesExtracter, List<AGG> aggregationList) {
        timeSeriesExtracter.setAggregationList(aggregationList);
    }

    private void fillingExtractorWithMetricsSizeInfo(MultiTimeSeriesExtracterImpl timeSeriesExtracter,
                                                     MetricsSizeInfo metricsInfo) {
        metricsInfo.getMetricRequests().forEach(MetricRequest -> {
            timeSeriesExtracter.setTotalNumberOfPointForMetric(MetricRequest, metricsInfo.getMetricInfo(MetricRequest).totalNumberOfPoints);
        });
    }

    private SamplingConf getSamplingConf(Request request) {
        SamplingAlgorithm algo = request.getSamplingAlgo();
        int bucketSize = request.getBucketSize();
        int maxPoint = request.getMaxPoint();
        return new SamplingConf(algo, bucketSize, maxPoint);
    }

    private JsonObject extractTimeSeriesThenBuildResponse(ChunkStream stream, MultiTimeSeriesExtracter timeSeriesExtracter) throws IOException {
        stream.open();
        Chunk chunk = stream.read();
        while (stream.hasNext()) {
            timeSeriesExtracter.addChunk(chunk);
            chunk = stream.read();
        }
        timeSeriesExtracter.flush();
        LOGGER.debug("read {} chunks in stream", stream.getCurrentNumberRead());
        LOGGER.debug("extractTimeSeries response metric : {}", chunk.toString());
        return buildTimeSeriesResponse(timeSeriesExtracter);
    }

    private JsonObject buildTimeSeriesResponse(MultiTimeSeriesExtracter timeSeriesExtracter) {
        return new JsonObject()
                .put(HistorianServiceFields.TOTAL_POINTS, timeSeriesExtracter.pointCount())
                .put(HistorianServiceFields.TIMESERIES, timeSeriesExtracter.getTimeSeries());
    }

    private ChunkStream queryStream(SolrQuery query) {
        StringBuilder exprBuilder = new StringBuilder("search(").append(solrHistorianConf.chunkCollection).append(",")
                .append("q=\"").append(query.getQuery()).append("\",");
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append("fq=\"").append(filterQuery).append("\",");
            }
        }
        exprBuilder
                .append("fl=\"").append(query.getFields()).append("\",")
                .append("sort=\"").append(query.getSortField()).append("\",")
                .append("qt=\"/export\")");

        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", exprBuilder.toString());
        paramsLoc.set("qt", "/stream");
        LOGGER.debug("queryStream params : {}", paramsLoc);

        TupleStream solrStream = new SolrStream(solrHistorianConf.streamEndPoint, paramsLoc);
        StreamContext context = new StreamContext();
        solrStream.setStreamContext(context);
        return ChunkStream.fromVersionAndSolrStream(solrHistorianConf.schemaVersion, solrStream);
    }

    private static class Request {

        JsonObject params;

        public Request(JsonObject params) {
            this.params = params;
        }

        public List<AGG> getAggs() {
            return params.getJsonArray(HistorianServiceFields.AGGREGATION, new JsonArray())
                    .stream()
                    .map(String::valueOf)
                    .map(AGG::valueOf)
                    .collect(Collectors.toList());
        }

        public Long getTo() {
            return params.getLong(HistorianServiceFields.TO);
        }

        public Long getFrom() {
            return params.getLong(HistorianServiceFields.FROM);
        }

        public Integer getMaxTotalChunkToRetrieve() {
            return params.getInteger(HistorianServiceFields.MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000);
        }

        public Map<String, String> getRootTags() {
            Map<String,String> tagsMap = new HashMap<>();
            params.getJsonObject(HistorianServiceFields.TAGS, new JsonObject()).getMap().forEach((key, value) -> tagsMap.put(key, value.toString()));
            return tagsMap;
        }

        /**
         * return the metric name desired with the associated tags.
         * The tags must be the result of the merge of specific tags and rootTags
         * @return
         */
        public List<MetricRequest> getMetricRequestsWithFinalTags() {
            return params.getJsonArray(HistorianServiceFields.NAMES).stream().map(i -> {
                try {
                    JsonObject metricObject = new JsonObject(i.toString());
                    String name = metricObject.getString(HistorianServiceFields.NAME);
                    Map<String,String> tagsMap = new HashMap<>();
                    getRootTags().forEach(tagsMap::put);
                    metricObject.getJsonObject(HistorianServiceFields.TAGS, new JsonObject()).getMap().forEach((key, value) -> tagsMap.put(key, value.toString()));
                    return new MetricRequest(name, tagsMap);
                }catch (Exception ex) {
                    String name = i.toString();
                    Map<String,String> tagsMap = new HashMap<>();
                    getRootTags().forEach(tagsMap::put);
                    return new MetricRequest(name, tagsMap);
                }
            })
            .collect(Collectors.toList());
        }

        public int getMaxPoint() {
            return params.getInteger(HistorianServiceFields.MAX_POINT_BY_METRIC);
        }

        public int getBucketSize() {
            return params.getInteger(HistorianServiceFields.BUCKET_SIZE);
        }

        public SamplingAlgorithm getSamplingAlgo() {
            return SamplingAlgorithm.valueOf(params.getString(HistorianServiceFields.SAMPLING_ALGO));
        }
    }
}
