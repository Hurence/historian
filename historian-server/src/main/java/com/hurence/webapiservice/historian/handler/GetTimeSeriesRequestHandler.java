package com.hurence.webapiservice.historian.handler;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.impl.*;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracter;
import com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtracterImpl;
import com.hurence.webapiservice.timeseries.extractor.MultiTimeSeriesExtractorUsingPreAgg;
import com.hurence.webapiservice.timeseries.extractor.TimeSeriesExtracterUtil;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class GetTimeSeriesRequestHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(GetTimeSeriesRequestHandler.class);
    SolrHistorianConf solrHistorianConf;

    public GetTimeSeriesRequestHandler(SolrHistorianConf solrHistorianConf) {
        this.solrHistorianConf = solrHistorianConf;
    }

    /**
     * nombre point < LIMIT_TO_DEFINE ==> Extract points from chunk
     * nombre point >= LIMIT_TO_DEFINE && nombre de chunk < LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg)
     * nombre de chunk >= LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg),
     * but should using agg on solr side (using key partition, by month, daily ? yearly ?)
     */
    public Handler<Promise<JsonObject>> getTimeSeriesHandler(JsonObject myParams) {
        final SolrQuery query = buildTimeSeriesQuery(myParams);
        LOGGER.debug("solrQuery : {}", query.toQueryString());

        //FILTER
        if (myParams.containsKey(HistorianFields.TAGS)) {
            Object tags = myParams.getValue(HistorianFields.TAGS);
            if (tags instanceof JsonObject) {
                buildSolrFilterFromTags(myParams.getJsonObject(HistorianFields.TAGS))
                        .ifPresent(query::addFilterQuery);
            } else if (tags instanceof JsonArray) {
                buildSolrFilterFromArray(myParams.getJsonArray(HistorianFields.TAGS), RESPONSE_TAG_NAME_FIELD)
                        .ifPresent(query::addFilterQuery);
            } else {
                throw new IllegalArgumentException(HistorianFields.TAGS + " field were neither a map neither an array.");
            }
        }
        List<AGG> aggregationList = myParams.getJsonArray(AGGREGATION, new JsonArray()).stream().map(String::valueOf).map(AGG::valueOf).collect(Collectors.toList());
        addFieldsThatWillBeNeededByAggregations(aggregationList, query);

        Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
            MetricsSizeInfo metricsInfo;
            try {
                metricsInfo = getNumberOfPointsByMetricInRequest(query);
                LOGGER.debug("metrics info to query : {}", metricsInfo);
                if (metricsInfo.isEmpty()) {
                    final MultiTimeSeriesExtracter timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(myParams, metricsInfo, aggregationList);
                    p.complete(buildTimeSeriesResponse(timeSeriesExtracter));
                    return;
                }
                final MultiTimeSeriesExtracter timeSeriesExtracter = getMultiTimeSeriesExtracter(myParams, query, metricsInfo, aggregationList);
                requestSolrAndBuildTimeSeries(query, p, timeSeriesExtracter);
            } catch (IOException e) {
                LOGGER.error("unexpected io exception", e);
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
        return null;
    }

    private SolrQuery buildTimeSeriesQuery(JsonObject params) {
        StringBuilder queryBuilder = new StringBuilder();
        if (params.getLong(TO) != null) {
            LOGGER.trace("requesting timeseries to {}", params.getLong(TO));
            queryBuilder.append(RESPONSE_CHUNK_START_FIELD).append(":[* TO ").append(params.getLong(TO)).append("]");
        }
        if (params.getLong(FROM) != null) {
            LOGGER.trace("requesting timeseries from {}", params.getLong(FROM));
            if (queryBuilder.length() != 0)
                queryBuilder.append(" AND ");
            queryBuilder.append(RESPONSE_CHUNK_END_FIELD).append(":[").append(params.getLong(FROM)).append(" TO *]");
        }
        //
        SolrQuery query = new SolrQuery("*:*");
        if (queryBuilder.length() != 0)
            query.setQuery(queryBuilder.toString());
        //TODO filter on names AND tags
//        buildSolrFilterFromArray(params.getJsonArray(NAMES), NAME)
//                .ifPresent(query::addFilterQuery);
        //    FIELDS_TO_FETCH
        query.setFields(RESPONSE_CHUNK_START_FIELD,
                RESPONSE_CHUNK_END_FIELD,
                RESPONSE_CHUNK_COUNT_FIELD,
                NAME);
        //    SORT
        query.setSort(RESPONSE_CHUNK_START_FIELD, SolrQuery.ORDER.asc);
        query.addSort(RESPONSE_CHUNK_END_FIELD, SolrQuery.ORDER.asc);
        query.setRows(params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000));
        return query;
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

    private MetricsSizeInfo getNumberOfPointsByMetricInRequest(SolrQuery query) throws IOException {//TODO better handling of exception
//        String cexpr = "rollup(search(historian, q=\"*:*\", fl=\"chunk_size, name\", qt=\"/export\", sort=\"name asc\"),\n" +
//                "\t\t\t\t over=\"name\", sum(chunk_size))";
        StringBuilder exprBuilder = new StringBuilder("rollup(search(").append(solrHistorianConf.chunkCollection)
                .append(",q=").append(query.getQuery());
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append(",fq=").append(filterQuery);
            }
        }
        exprBuilder.append(",fl=\"").append(RESPONSE_CHUNK_COUNT_FIELD).append(", ")
                .append(NAME).append("\"")
                .append(",qt=\"/export\", sort=\"").append(NAME).append(" asc\")")
                .append(",over=\"").append(NAME).append("\"")
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
            MetricSizeInfo metric = new MetricSizeInfo();
            metric.metricName = tuple.getString("name");
            metric.totalNumberOfChunks = tuple.getLong("count(*)");
            metric.totalNumberOfPoints = tuple.getLong("sum(chunk_count)");
            metricsInfo.setMetricInfo(metric);
            tuple = solrStream.read();
        }
        LOGGER.debug("metric response : {}", tuple.jsonStr());
        solrStream.close(); //TODO could be try-with-resources
        return metricsInfo;
    }


    public MultiTimeSeriesExtracter getMultiTimeSeriesExtracter(JsonObject myParams, SolrQuery query, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        //TODO make three different group for each metrics, not use a single strategy globally for all metrics.
        final MultiTimeSeriesExtracter timeSeriesExtracter;
        if (metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint ||
                metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(myParams).getMaxPoint()) {
            LOGGER.debug("QUERY MODE 1: metricsInfo.getTotalNumberOfPoints() < limitNumberOfPoint");
            query.addField(RESPONSE_CHUNK_VALUE_FIELD);
            timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(myParams, metricsInfo, aggregationList);
        } else if (metricsInfo.getTotalNumberOfChunks() < solrHistorianConf.limitNumberOfChunks) {
            LOGGER.debug("QUERY MODE 2: metricsInfo.getTotalNumberOfChunks() < limitNumberOfChunks");
            addFieldsThatWillBeNeededBySamplingAlgorithms(myParams, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(myParams, metricsInfo, aggregationList);
        } else {
            LOGGER.debug("QUERY MODE 3 : else");
            //TODO Sample points with chunk aggs depending on alg (min, avg),
            // but should using agg on solr side (using key partition, by month, daily ? yearly ?)
            // For the moment we use the stream api without partitionning
            addFieldsThatWillBeNeededBySamplingAlgorithms(myParams, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(myParams, metricsInfo, aggregationList);
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

    public void addFieldsThatWillBeNeededBySamplingAlgorithms(JsonObject myParams, SolrQuery query, MetricsSizeInfo metricsInfo) {
        SamplingConf requestedSamplingConf = getSamplingConf(myParams);
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
        return metricsSizeInfo.getMetrics().stream()
                .map(metricName -> {
                    MetricSizeInfo metricInfo = metricsSizeInfo.getMetricInfo(metricName);
                    SamplingAlgorithm algo = TimeSeriesExtracterUtil.calculSamplingAlgorithm(askedSamplingConf, metricInfo.totalNumberOfPoints);
                    return algo;
                }).collect(Collectors.toSet());
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtractorUsingPreAgg
    private MultiTimeSeriesExtracter createTimeSerieExtractorUsingChunks(JsonObject params, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        long from = params.getLong(FROM);
        long to = params.getLong(TO);
        SamplingConf requestedSamplingConf = getSamplingConf(params);
        MultiTimeSeriesExtractorUsingPreAgg timeSeriesExtracter = new MultiTimeSeriesExtractorUsingPreAgg(from, to, requestedSamplingConf);
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtracterImpl
    private MultiTimeSeriesExtracter createTimeSerieExtractorSamplingAllPoints(JsonObject params, MetricsSizeInfo metricsInfo, List<AGG> aggregationList) {
        long from = params.getLong(FROM);
        long to = params.getLong(TO);
        SamplingConf requestedSamplingConf = getSamplingConf(params);
        MultiTimeSeriesExtracterImpl timeSeriesExtracter = new MultiTimeSeriesExtracterImpl(from, to, requestedSamplingConf);
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        fillingExtractorWithAggregToReturn(timeSeriesExtracter,aggregationList);
        return timeSeriesExtracter;
    }

    private void fillingExtractorWithAggregToReturn(MultiTimeSeriesExtracterImpl timeSeriesExtracter, List<AGG> aggregationList) {
        timeSeriesExtracter.setAggregationList(aggregationList);
    }

    private void fillingExtractorWithMetricsSizeInfo(MultiTimeSeriesExtracterImpl timeSeriesExtracter,
                                                     MetricsSizeInfo metricsInfo) {
        metricsInfo.getMetrics().forEach(metric -> {
            timeSeriesExtracter.setTotalNumberOfPointForMetric(metric, metricsInfo.getMetricInfo(metric).totalNumberOfPoints);
        });
    }

    private SamplingConf getSamplingConf(JsonObject params) {
        SamplingAlgorithm algo = SamplingAlgorithm.valueOf(params.getString(SAMPLING_ALGO));
        int bucketSize = params.getInteger(BUCKET_SIZE);
        int maxPoint = params.getInteger(MAX_POINT_BY_METRIC);
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
}
