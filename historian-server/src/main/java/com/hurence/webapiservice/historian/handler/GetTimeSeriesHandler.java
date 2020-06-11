package com.hurence.webapiservice.historian.handler;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.impl.*;
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

import static com.hurence.historian.modele.HistorianFields.*;

public class GetTimeSeriesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTimeSeriesHandler.class);
    SolrHistorianConf solrHistorianConf;

    public GetTimeSeriesHandler(SolrHistorianConf solrHistorianConf) {
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
                metricsInfo = getNumberOfPointsByMetricInRequest(request.getMetricRequests(), query);
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
            queryBuilder.append(RESPONSE_CHUNK_START_FIELD).append(":[* TO ").append(request.getTo()).append("]");
        }
        if (request.getFrom()  != null) {
            LOGGER.trace("requesting timeseries from {}", request.getFrom());
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(RESPONSE_CHUNK_END_FIELD).append(":[").append(request.getFrom()).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());

        //FILTER
        buildFilters(request, query);
        //    FIELDS_TO_FETCH
        query.setFields(RESPONSE_CHUNK_START_FIELD,
                RESPONSE_CHUNK_END_FIELD,
                RESPONSE_CHUNK_COUNT_FIELD,
                NAME);
        addFieldsThatWillBeNeededByAggregations(request.getAggs(), query);
        //    SORT
        query.setSort(RESPONSE_CHUNK_START_FIELD, SolrQuery.ORDER.asc);
        query.addSort(RESPONSE_CHUNK_END_FIELD, SolrQuery.ORDER.asc);
        query.setRows(request.getMaxTotalChunkToRetrieve());

        return query;
    }

    private void buildFilters(Request request, SolrQuery query) {
        Map<String, String> rootTags = request.getRootTags();
        List<MetricRequest> metricOptions = request.getMetricRequests();
        List<String> metricFilters = buildFilterForEachMetric(rootTags, metricOptions);
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
        //TODO
        return null;
    }

    /**
     * filter query for each metric
     *     each string should looks like "(name:A && usine:usine_1 && sensor:sensor_1)"
     *     we filter on metric name and on every tags we get. we use tags of metricOptions and of rootTags.
     *     If there is conflict then tags of metricOptions have priority.
     * @param rootTags
     * @param metricOptions
     * @return filter query for each metric
     *         each string should looks like :
     *         <pre>
     *             name:"A" AND usine:"usine_1" AND sensor:"sensor_1"
     *         </pre>
     *
     *
     */
    private List<String> buildFilterForEachMetric(Map<String, String> rootTags, List<MetricRequest> metricOptions) {
        return null;//TODO
    }

    private Optional<String> buildSolrFilterFromArray(JsonArray jsonArray, String fieldToFilter) {
        if (jsonArray == null || jsonArray.isEmpty())
            return Optional.empty();
        if (jsonArray.size() == 1) {
            return Optional.of(fieldToFilter + ":\"" + jsonArray.getString(0) + "\"");
        } else {
            String orNames = jsonArray.stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining("\" OR \"", "(\"", "\")"));
            return Optional.of(fieldToFilter + ":" + orNames);
        }
    }

    private Optional<String> buildSolrFilterFromTags(JsonObject tags) {
        if (tags == null || tags.isEmpty())
            return Optional.empty();
        String filters = tags.fieldNames().stream()
                .map(tagName -> {
                    String value = tags.getString(tagName);
                    return tagName + ":\"" + value + "\"";
//                    return "\"" + tagName + "\":\"" + value + "\"";
                })
                .collect(Collectors.joining(" AND ", "", ""));
        return Optional.of(filters);
    }

    private void addFieldsThatWillBeNeededByAggregations(List<AGG> aggregationList, SolrQuery query) {
        aggregationList.forEach(agg -> {
            switch (agg) {
                case AVG:
                    query.addField(RESPONSE_CHUNK_AVG_FIELD);
                    break;
                case SUM:
                    query.addField(RESPONSE_CHUNK_SUM_FIELD);
                    break;
                case MIN:
                    query.addField(RESPONSE_CHUNK_MIN_FIELD);
                    break;
                case MAX:
                    query.addField(RESPONSE_CHUNK_MAX_FIELD);
                    break;
                case COUNT:
                    query.addField(RESPONSE_CHUNK_COUNT_FIELD);
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
        neededFields.add(NAME);
        List<String> overFields = new ArrayList<>(neededFields);
        String overString = joinListAsString(overFields);
        neededFields.add(RESPONSE_CHUNK_COUNT_FIELD);
        String flString = joinListAsString(neededFields);
        exprBuilder.append(",fl=\"").append(flString).append("\"")
                .append(",qt=\"/export\", sort=\"").append(NAME).append(" asc\")")
                .append(",over=\"").append(overString).append("\"")
                .append(", sum(").append(RESPONSE_CHUNK_COUNT_FIELD).append("), count(*))");
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
            LOGGER.trace("tuple : {}", tuple.jsonStr());
            for (MetricRequest request: requests) {
                if (isTupleMatchingMetricRequest(request, tuple)) {
                    metricsInfo.increaseNumberOfChunksForMetricRequest(request, tuple.getLong("count(*)"));
                    metricsInfo.increaseNumberOfPointsForMetricRequest(request, tuple.getLong("sum(chunk_count)"));
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
        return null;
    }

    /**
     * return all tags name needed for querying metrics.
     * @param requests
     * @return
     */
    private List<String> findNeededTagsName(List<MetricRequest> requests) {
        //TODO
        return null;
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
        //TODO
        return false;
    }


    public MultiTimeSeriesExtracter getMultiTimeSeriesExtracter(Request request, SolrQuery query, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        //TODO make three different group for each metrics, not use a single strategy globally for all metrics.
        final MultiTimeSeriesExtracter timeSeriesExtracter;
        if (metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint ||
                metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(request).getMaxPoint()) {
            LOGGER.debug("QUERY MODE 1: metricsInfo.getTotalNumberOfPoints() < limitNumberOfPoint");
            query.addField(RESPONSE_CHUNK_VALUE_FIELD);
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
        try (JsonStream stream = queryStream(query)) {
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
                    query.addField(RESPONSE_CHUNK_VALUE_FIELD);
                    break;
                case FIRST:
                    query.addField(RESPONSE_CHUNK_FIRST_VALUE_FIELD);
                    break;
                case AVERAGE:
                    query.addField(RESPONSE_CHUNK_SUM_FIELD);
                    break;
                case MIN:
                    query.addField(RESPONSE_CHUNK_MIN_FIELD);
                    break;
                case MAX:
                    query.addField(RESPONSE_CHUNK_MAX_FIELD);
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
            Set<SamplingAlgorithm> singletonSet = new HashSet<SamplingAlgorithm>();
            singletonSet.add(askedSamplingConf.getAlgo());
            return singletonSet;
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
        MultiTimeSeriesExtractorUsingPreAgg timeSeriesExtracter = new MultiTimeSeriesExtractorUsingPreAgg(from, to, requestedSamplingConf, request.getMetricRequests());
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtracterImpl
    private MultiTimeSeriesExtracter createTimeSerieExtractorSamplingAllPoints(Request request, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        long from = request.getFrom();
        long to = request.getTo();
        SamplingConf requestedSamplingConf = getSamplingConf(request);
        MultiTimeSeriesExtracterImpl timeSeriesExtracter = new MultiTimeSeriesExtracterImpl(from, to, requestedSamplingConf, request.getMetricRequests());
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

    private JsonObject extractTimeSeriesThenBuildResponse(JsonStream stream, MultiTimeSeriesExtracter timeSeriesExtracter) throws IOException {
        stream.open();
        JsonObject chunk = stream.read();
        while (!chunk.containsKey("EOF") || !chunk.getBoolean("EOF")) {
            timeSeriesExtracter.addChunk(chunk);
            chunk = stream.read();
        }
        timeSeriesExtracter.flush();
        LOGGER.debug("read {} chunks in stream", stream.getNumberOfDocRead());
        LOGGER.debug("extractTimeSeries response metric : {}", chunk.encodePrettily());
        return buildTimeSeriesResponse(timeSeriesExtracter);
    }

    private JsonObject extractTimeSeriesThenBuildResponse(List<JsonObject> chunks, MultiTimeSeriesExtracter timeSeriesExtracter) {
        chunks.forEach(timeSeriesExtracter::addChunk);
        timeSeriesExtracter.flush();
        return buildTimeSeriesResponse(timeSeriesExtracter);
    }

    private JsonObject buildTimeSeriesResponse(MultiTimeSeriesExtracter timeSeriesExtracter) {
        return new JsonObject()
                .put(TOTAL_POINTS, timeSeriesExtracter.pointCount())
                .put(TIMESERIES, timeSeriesExtracter.getTimeSeries());
    }

    private JsonStream queryStream(SolrQuery query) {
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
        return JsonStreamSolrStream.forVersion(solrHistorianConf.schemaVersion, solrStream);
    }

    private static class Request {

        JsonObject params;

        public Request(JsonObject params) {
            this.params = params;
        }

        public List<AGG> getAggs() {
            return params.getJsonArray(AGGREGATION, new JsonArray())
                    .stream()
                    .map(String::valueOf)
                    .map(AGG::valueOf)
                    .collect(Collectors.toList());
        }

        public Long getTo() {
            return params.getLong(TO);
        }

        public Long getFrom() {
            return params.getLong(FROM);
        }

        public Integer getMaxTotalChunkToRetrieve() {
            return params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000);
        }

        public Map<String, String> getRootTags() {
            //TODO
            return null;
        }

        public List<MetricRequest> getMetricRequests() {
            //TODO
            return null;
        }

        public int getMaxPoint() {
            return params.getInteger(MAX_POINT_BY_METRIC);
        }

        public int getBucketSize() {
            return params.getInteger(BUCKET_SIZE);
        }

        public SamplingAlgorithm getSamplingAlgo() {
            return SamplingAlgorithm.valueOf(params.getString(SAMPLING_ALGO));
        }
    }
}
