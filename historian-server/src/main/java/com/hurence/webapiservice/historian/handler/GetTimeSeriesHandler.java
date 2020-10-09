package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.model.HistorianConf;
import com.hurence.historian.model.HistorianServiceFields;
import com.hurence.historian.model.solr.SolrFieldMapping;
import com.hurence.historian.model.stream.ChunkStream;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.sampling.SamplingAlgorithm;

import com.hurence.webapiservice.historian.impl.*;
import com.hurence.webapiservice.http.api.grafana.util.QualityAgg;
import com.hurence.webapiservice.http.api.grafana.util.QualityConfig;

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

import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.*;

public class GetTimeSeriesHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTimeSeriesHandler.class);
    HistorianConf historianConf;
    SolrHistorianConf solrHistorianConf;

    public GetTimeSeriesHandler(HistorianConf historianConf, SolrHistorianConf solrHistorianConf) {
        this.historianConf = historianConf;
        this.solrHistorianConf = solrHistorianConf;
    }


    /**
     * nombre point < LIMIT_TO_DEFINE ==> Extract measures from chunk
     * nombre point >= LIMIT_TO_DEFINE && nombre de chunk < LIMIT_TO_DEFINE ==> Sample measures with chunk aggs depending on alg (min, avg)
     * nombre de chunk >= LIMIT_TO_DEFINE ==> Sample measures with chunk aggs depending on alg (min, avg),
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
                metricsInfo = getNumberOfPointsByMetricInRequest(request.getMetricRequestsWithFinalTagsAndFinalQualities(), query, request.getUseQuality());
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
            queryBuilder.append(SOLR_COLUMN_START).append(":[* TO ").append(request.getTo()).append("]");
        }
        if (request.getFrom()  != null) {
            LOGGER.trace("requesting timeseries from {}", request.getFrom());
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(SOLR_COLUMN_END).append(":[").append(request.getFrom()).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());

        //FILTER
        buildFilters(request, query, false);
        //    FIELDS_TO_FETCH
        query.setFields(SOLR_COLUMN_START,
                SOLR_COLUMN_END,
                SOLR_COLUMN_COUNT,
                SOLR_COLUMN_NAME);
        addQualityFields(query, request);
        addAllTagsAsFields(request.getMetricRequestsWithFinalTagsAndFinalQualities(), query);

        addFieldsThatWillBeNeededByAggregations(request.getAggs(), query);
        //    SORT
        query.setSort(SOLR_COLUMN_START, SolrQuery.ORDER.asc);
        query.addSort(SOLR_COLUMN_END, SolrQuery.ORDER.asc);
        query.setRows(request.getMaxTotalChunkToRetrieve());

        return query;
    }


    private void addQualityFields(SolrQuery query, Request request) {
        Set<String> neededQualityFields = getNeededQualityFields(request);
        neededQualityFields.forEach(query::addField);
    }

    private Set<String> getNeededQualityFields(Request request) {
        final Set<String> neededQualityFields = new HashSet<>();
        request.getMetricRequestsWithFinalTagsAndFinalQualities().forEach(
                metricRequest -> {
                    if (!metricRequest.getQuality().getQualityAgg().equals(QualityAgg.NONE))
                        neededQualityFields.add(metricRequest.getQuality().getChunkQualityFieldForSampling());
                });
        return neededQualityFields;
    }


    private SolrFieldMapping getHistorianFields() {
        return this.historianConf.getFieldsInSolr();

    }

    private void addAllTagsAsFields(List<MetricRequest> metricRequests, SolrQuery query) {
        Set<String> tags = new HashSet<>();
        metricRequests.forEach(metricRequest -> tags.addAll(metricRequest.getTags().keySet()));
        tags.forEach(query::addField);
    }

    private void buildFilters(Request request, SolrQuery query, boolean qualityMatterInQuery) {
        List<MetricRequest> metricOptions = request.getMetricRequestsWithFinalTagsAndFinalQualities();
        List<String> metricFilters = buildFilterForEachMetric(metricOptions, request.getUseQuality() && qualityMatterInQuery);
        String finalFilter = buildFinalFilterQuery(metricFilters);
        query.setFilterQueries(finalFilter);
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
     *     we filter on metric name and on every tags we get. we use tags of metricRequests and of rootTags.
     *     If there is conflict then tags of metricRequests have priority.
     * @param metricRequests
     * @return filter query for each metric
     *         each string should looks like :
     *         <pre>
     *             name:"A" AND usine:"usine_1" AND sensor:"sensor_1"
     *         </pre>
     *
     *
     */
    //TODO
    private List<String> buildFilterForEachMetric(List<MetricRequest> metricRequests,
                                                  boolean useQuality) {
        List<String> finalStringList = new ArrayList<>();
        metricRequests.forEach(metricRequest -> {
            StringBuilder queryForEachMetricBuilder = new StringBuilder();
            List<String> tagsFilter = getTagsAsPair(metricRequest);
            if(!tagsFilter.isEmpty())
                queryForEachMetricBuilder.append(tagsFilter.stream().collect(Collectors.joining(" AND ", SOLR_COLUMN_NAME +":\""+metricRequest.getName()+"\" AND ", "")));
            else
                queryForEachMetricBuilder.append(SOLR_COLUMN_NAME + ":\""+metricRequest.getName()+"\"");
            if (useQuality) {
                Float qualityValue = metricRequest.getQuality().getQualityValue();
                String qualityField = SOLR_COLUMN_QUALITY_AVG;//at the moment always use AVG to filter
                queryForEachMetricBuilder.append(" AND ").append(qualityField).append(":[").append(qualityValue).append(" TO *]"); // TODO isn't TO 1 better ?
            }
            finalStringList.add(queryForEachMetricBuilder.toString());
        });
        return finalStringList;
    }

    private List<String> getTagsAsPair(MetricRequest metricRequest) {
        List<String> tagsFilter = new ArrayList<>();
        metricRequest.getTags().forEach((key, value) -> {
            tagsFilter.add(key+":\""+value+"\"");
        });
        return tagsFilter;
    }


    private void addFieldsThatWillBeNeededByAggregations(List<AGG> aggregationList, SolrQuery query) {
        aggregationList.forEach(agg -> {
            switch (agg) {
                case AVG:
                    query.addField(SOLR_COLUMN_AVG);
                    break;
                case SUM:
                    query.addField(SOLR_COLUMN_SUM);
                    break;
                case MIN:
                    query.addField(SOLR_COLUMN_MIN);
                    break;
                case MAX:
                    query.addField(SOLR_COLUMN_MAX);
                    break;
                case COUNT:
                    query.addField(SOLR_COLUMN_COUNT);
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
    }


    private MetricsSizeInfo getNumberOfPointsByMetricInRequest(List<MetricRequest> requests, SolrQuery query, boolean useQuality) throws IOException {
        NumberOfPointsByMetricHelper numberOfPointsByMetricHelper = getNumberOfPointsByMetricHelperImpl(useQuality, requests, query);
        String preRequestExpr = numberOfPointsByMetricHelper.getStreamExpression();
        LOGGER.trace("expression is : {}", preRequestExpr);
        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set("expr", preRequestExpr);
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
                            tuple.getLong("sum(" + SOLR_COLUMN_COUNT + ")")
                    );
                    if (useQuality && tuple.getBool(HistorianServiceFields.QUALITY_CHECK)) {
                        metricsInfo.increaseNumberOfChunksWithQualityOkForMetricRequest(request, tuple.getLong("count(*)"));
                        metricsInfo.increaseNumberOfPointsWithQualityOkForMetricRequest(request, tuple.getLong("sum(" + SOLR_COLUMN_COUNT + ")"));
                    }
                }
            }
            tuple = solrStream.read();
        }
        LOGGER.debug("metric response : {}", tuple.jsonStr());
        solrStream.close();
        return metricsInfo;
    }

    private NumberOfPointsByMetricHelper getNumberOfPointsByMetricHelperImpl(boolean useQuality, List<MetricRequest> requests, SolrQuery query) {
        if (useQuality)
            return new NumberOfPointsWithQualityOkByMetricHelperImpl(getHistorianFields(),
                    solrHistorianConf.chunkCollection, query, requests);
        else
            return new NumberOfAllPointsByMetricHelperImpl(getHistorianFields(),
                    solrHistorianConf.chunkCollection, query, requests);
    }

    /**
     * join string so that we got somthing like <pre>elem1,elem2,elem3</pre>
     * @param neededFields
     * @return
     */
    public static String joinListAsString(List<String> neededFields) {
        return String.join(",", neededFields);
    }

    /**
     * return all tags name needed for querying metrics.
     * @param requests
     * @return
     */
    public static List<String> findNeededTagsName(List<MetricRequest> requests) {
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
     * @param tuple the count of chunks and measures for all chunk matching the query.
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
        if (!request.getName().equals(tuple.fields.get(SOLR_COLUMN_NAME))){
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
        //Now CHUNK_VALUE_FIELD may potentially be needed by all mode. In case we must recompute chunks !
        //Indeed if user query data from t1 to t3 and chunk contains data from t2 and t4 we must recompute the chunk
        //by removing data after t3 !
        query.addField(SOLR_COLUMN_VALUE);
        if (isQueryMode1(request, metricsInfo)) {
            LOGGER.debug("QUERY MODE 1");
            timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(request, metricsInfo, aggregationList);
        } else if (isQueryMode2ConsideringNotQueryMode1(request, metricsInfo)) {
            LOGGER.debug("QUERY MODE 2");
            buildFilters(request, query, true);
            addFieldsThatWillBeNeededBySamplingAlgorithms(request, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(request, metricsInfo, aggregationList);
        } else {
            LOGGER.debug("QUERY MODE 3 : else");
            //TODO Sample measures with chunk aggs depending on alg (min, avg),
            LOGGER.debug("QUERY MODE 3");
            //TODO Sample points with chunk aggs depending on alg (min, avg),
            // but should using agg on solr side (using key partition, by month, daily ? yearly ?)
            // For the moment we use the stream api without partitionning
            addFieldsThatWillBeNeededBySamplingAlgorithms(request, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(request, metricsInfo, aggregationList);
        }
        return timeSeriesExtracter;
    }

    /**
     * We suppose that we already know that this is not QUERY MODE 1
     * @param metricsInfo
     * @return true if we should use query mode 2 (sampling with pre agg)
     */
    private boolean isQueryMode2ConsideringNotQueryMode1(Request request, MetricsSizeInfo metricsInfo) {
        if (!request.getUseQuality()) {
            return metricsInfo.getTotalNumberOfChunks() < solrHistorianConf.limitNumberOfChunks;
        } else {
            return metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < solrHistorianConf.limitNumberOfChunks;
        }
    }
    /**
     *
     * @param metricsInfo
     * @return true if we should use query mode 1 not using pre agg and decompressing all chunks.
     */
    private boolean isQueryMode1(Request request, MetricsSizeInfo metricsInfo) {
        boolean isQueryMode1;
        if (!request.getUseQuality()) {
            isQueryMode1 = metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint ||
                    metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(request).getMaxPoint() ||
                    metricsInfo.getTotalNumberOfChunks() < getSamplingConf(request).getMaxPoint();
            if (LOGGER.isDebugEnabled() && isQueryMode1) {
                String queryModeMsg = "QUERY MODE 1 because : \n";
                if (metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint) {
                    queryModeMsg += "metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint\n";
                }
                if (metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(request).getMaxPoint()) {
                    queryModeMsg += "metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(request).getMaxPoint()\n";
                }
                if (metricsInfo.getTotalNumberOfChunks() < getSamplingConf(request).getMaxPoint()) {
                    queryModeMsg += "metricsInfo.getTotalNumberOfChunks() < getSamplingConf(request).getMaxPoint()\n";
                }
                LOGGER.debug(queryModeMsg);
            }
        } else {
            isQueryMode1 = metricsInfo.getTotalNumberOfPointsWithCorrectQuality() < solrHistorianConf.limitNumberOfPoint ||
                    metricsInfo.getTotalNumberOfPointsWithCorrectQuality() <= getSamplingConf(request).getMaxPoint() ||
                    metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < getSamplingConf(request).getMaxPoint();
            if (LOGGER.isDebugEnabled() && isQueryMode1) {
                String queryModeMsg = "QUERY MODE 1 because : \n";
                if (metricsInfo.getTotalNumberOfPointsWithCorrectQuality() < solrHistorianConf.limitNumberOfPoint) {
                    queryModeMsg += "metricsInfo.getTotalNumberOfPointsWithCorrectQuality() < solrHistorianConf.limitNumberOfPoint\n";
                }
                if (metricsInfo.getTotalNumberOfPointsWithCorrectQuality() <= getSamplingConf(request).getMaxPoint()) {
                    queryModeMsg += "metricsInfo.getTotalNumberOfPointsWithCorrectQuality() <= getSamplingConf(request).getMaxPoint()\n";
                }
                if (metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < getSamplingConf(request).getMaxPoint()) {
                    queryModeMsg += "metricsInfo.getTotalNumberOfChunksWithCorrectQuality() < getSamplingConf(request).getMaxPoint()\n";
                }
                LOGGER.debug(queryModeMsg);
            }
        }
        return isQueryMode1;
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
        if (request.getQualityReturn()) {
            addNecessaryQualityFieldToQuery(request, query, samplingAlgos);
        }
    }


    private void addNecessaryQualityFieldToQuery(Request request, SolrQuery query, Set<SamplingAlgorithm> samplingAlgos) {
        /*if(request.getUseQuality())*/
        samplingAlgos.forEach(algo -> {
            switch (algo) {
                case NONE:
                    break;
                case FIRST:
                    query.addField(SOLR_COLUMN_QUALITY_FIRST);
                    break;
                case AVERAGE:
                    query.addField(SOLR_COLUMN_QUALITY_AVG);
                    break;
                case MIN:
                    query.addField(SOLR_COLUMN_QUALITY_MIN);
                    break;
                case MAX:
                    query.addField(SOLR_COLUMN_QUALITY_MAX);
                    break;
                default:
                    throw new IllegalStateException("algorithm " + algo.name() + " is not yet supported !");
            }
        });
    }


    private void addNecessaryFieldToQuery(SolrQuery query, Set<SamplingAlgorithm> samplingAlgos) {
        samplingAlgos.forEach(algo -> {
            switch (algo) {
                case NONE:
                    query.addField(SOLR_COLUMN_VALUE);
                    break;
                case FIRST:
                    query.addField(SOLR_COLUMN_FIRST);
                    break;
                case AVERAGE:
                    query.addField(SOLR_COLUMN_SUM);
                    break;
                case MIN:
                    query.addField(SOLR_COLUMN_MIN);
                    break;
                case MAX:
                    query.addField(SOLR_COLUMN_MAX);
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
        MultiTimeSeriesExtractorUsingPreAgg timeSeriesExtracter = new MultiTimeSeriesExtractorUsingPreAgg(from, to, requestedSamplingConf, request.getMetricRequestsWithFinalTagsAndFinalQualities(), request.getQualityReturn());
        if (request.getUseQuality()) {
            metricsInfo.getMetricRequests().forEach(metricRequest -> {
                timeSeriesExtracter.setTotalNumberOfPointForMetric(metricRequest, metricsInfo.getMetricInfo(metricRequest).totalNumberOfPointsWithCorrectQuality);
            });
        } else {
            metricsInfo.getMetricRequests().forEach(metricRequest -> {
                timeSeriesExtracter.setTotalNumberOfPointForMetric(metricRequest, metricsInfo.getMetricInfo(metricRequest).totalNumberOfPoints);
            });
        }
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtracterImpl
    private MultiTimeSeriesExtracter createTimeSerieExtractorSamplingAllPoints(Request request, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        long from = request.getFrom();
        long to = request.getTo();
        SamplingConf requestedSamplingConf = getSamplingConf(request);
        MultiTimeSeriesExtracterImpl timeSeriesExtracter = new MultiTimeSeriesExtracterImpl(from, to, requestedSamplingConf, request.getMetricRequestsWithFinalTagsAndFinalQualities(), request.getQualityReturn());
        metricsInfo.getMetricRequests().forEach(metricRequest -> {
            timeSeriesExtracter.setTotalNumberOfPointForMetric(metricRequest, metricsInfo.getMetricInfo(metricRequest).totalNumberOfPoints);
        });
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    private void fillingExtractorWithAggregToReturn(MultiTimeSeriesExtracterImpl timeSeriesExtracter, List<AGG> aggregationList) {
        timeSeriesExtracter.setAggregationList(aggregationList);
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
        LOGGER.debug("read {} chunks in stream", stream.getCurrentNumberRead() - 1);//one doc is EOF
        LOGGER.debug("extractTimeSeries response metric : {}", chunk.toString());

        return buildTimeSeriesResponse(timeSeriesExtracter);
    }

    private JsonObject buildTimeSeriesResponse(MultiTimeSeriesExtracter timeSeriesExtracter) {
        return new JsonObject()
                .put(TOTAL_POINTS, timeSeriesExtracter.pointCount())
                .put(TIMESERIES, timeSeriesExtracter.getTimeSeries());
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
            return params.getJsonArray(AGGREGATION, new JsonArray())
                    .stream()
                    .map(String::valueOf)
                    .map(AGG::valueOf)
                    .collect(Collectors.toList());
        }

        public boolean getUseQuality() { return params.getBoolean(USE_QUALITY, false);}

        public Long getTo() {
            return params.getLong(TO);
        }

        public Long getFrom() {
            return params.getLong(FROM);
        }

        public boolean getQualityReturn() {
            return params.getBoolean(QUALITY_RETURN, false);
        }

        public Integer getMaxTotalChunkToRetrieve() {
            return params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000);
        }

        public Map<String, String> getRootTags() {
            Map<String,String> tagsMap = new HashMap<>();
            params.getJsonObject(FIELD_TAGS, new JsonObject()).getMap().forEach((key, value) -> tagsMap.put(key, value.toString()));
            return tagsMap;
        }

        /**
         * return the metric name desired with the associated tags and the associated quality (if quality exist).
         * The tags must be the result of the merge of specific tags and rootTags
         * The quality must be the result of the merge of specific quality and rootQuality
         * @return
         */
        public List<MetricRequest> getMetricRequestsWithFinalTagsAndFinalQualities() {
            return params.getJsonArray(NAMES).stream().map(i -> {
                try {
                    JsonObject metricObject = new JsonObject(i.toString());
                    String name = metricObject.getString(SOLR_COLUMN_NAME);
                    Float qualityValue = metricObject.getFloat(QUALITY_VALUE, getRootQuality().getQualityValue());
                    String qualityAgg = getRootQuality().getQualityAgg().toString();
                    Map<String,String> tagsMap = new HashMap<>();
                    getRootTags().forEach(tagsMap::put);
                    metricObject.getJsonObject(FIELD_TAGS, new JsonObject()).getMap().forEach((key, value) -> tagsMap.put(key, value.toString()));
                    QualityConfig qualityConfig = new QualityConfig(qualityValue, qualityAgg);
                    return new MetricRequest(name, tagsMap, qualityConfig);
                }catch (Exception ex) {
                    String name = i.toString();
                    Map<String,String> tagsMap = new HashMap<>();
                    getRootTags().forEach(tagsMap::put);
                    QualityConfig qualityConfig = getRootQuality();
                    return new MetricRequest(name, tagsMap, qualityConfig);
                }
            })
            .collect(Collectors.toList());
        }

        public QualityConfig getRootQuality() {
            return new QualityConfig(
                    params.getFloat(QUALITY_VALUE, Float.NaN),
                    params.getString(QUALITY_AGG, QualityAgg.NONE.toString())
            ) ;
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
