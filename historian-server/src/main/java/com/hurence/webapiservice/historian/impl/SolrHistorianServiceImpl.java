package com.hurence.webapiservice.historian.impl;

import com.google.common.hash.Hashing;
import com.hurence.historian.modele.HistorianFields;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.TimeSeriesRecord;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.HistorianService;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestType;
import com.hurence.webapiservice.http.api.ingestion.JsonObjectToChunk;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.MultiTimeSeriesExtracter;
import com.hurence.webapiservice.timeseries.MultiTimeSeriesExtracterImpl;
import com.hurence.webapiservice.timeseries.MultiTimeSeriesExtractorUsingPreAgg;
import com.hurence.webapiservice.timeseries.TimeSeriesExtracterUtil;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;

public class SolrHistorianServiceImpl implements HistorianService {

    private static Logger LOGGER = LoggerFactory.getLogger(SolrHistorianServiceImpl.class);

    private final Vertx vertx;
    private final SolrHistorianConf solrHistorianConf;

    public SolrHistorianServiceImpl(Vertx vertx, SolrHistorianConf solrHistorianConf,
                                    Handler<AsyncResult<HistorianService>> readyHandler) {
        this.vertx = vertx;
        this.solrHistorianConf = solrHistorianConf;
        LOGGER.debug("SolrHistorianServiceImpl with params:");
        LOGGER.debug("collections : {} for chunks and {} for annotations", solrHistorianConf.chunkCollection, solrHistorianConf.annotationCollection);
        LOGGER.debug("streamEndPoint : {}", solrHistorianConf.streamEndPoint);
        LOGGER.debug("limitNumberOfPoint : {}", solrHistorianConf.limitNumberOfPoint);
        LOGGER.debug("limitNumberOfChunks : {}", solrHistorianConf.limitNumberOfChunks);
        LOGGER.debug("version of schema to be used : {}", solrHistorianConf.schemaVersion.toString());
        Handler<Promise<Integer>> colPinghandler = createPingHandler(solrHistorianConf.sleepDurationBetweenTry, solrHistorianConf.numberOfRetryToConnect);
        Handler<AsyncResult<Integer>> statusHandler = h -> {
            if (h.succeeded()) {
                if (h.result() == 0) {
                    readyHandler.handle(Future.succeededFuture(this));
                } else {
                    readyHandler.handle(Future.failedFuture(new IllegalArgumentException(
                            String.format("historian collection ping command returned status %d", h.result())
                    )));
                }
            } else {
                readyHandler.handle(Future.failedFuture(h.cause()));
            }
        };
        vertx.executeBlocking(colPinghandler, statusHandler);
    }

    private Handler<Promise<Integer>> createPingHandler(long sleepDurationMilli, int numberOfRetry) {
        return p -> {
            try {
                p.complete(pingSolrServer(6000, 3));
            } catch (IOException e) {
                LOGGER.error("IOException while pinging solr", e);
                p.fail(e);
            } catch (SolrServerException e) {
                LOGGER.error("SolrServerException while pinging solr", e);
                p.fail(e);
            }
        };
    }

    private Integer pingSolrServer(long sleepDurationMilli, int numberOfRetry) throws IOException, SolrServerException {
        try {
            final SolrRequest request = CollectionAdminRequest.collectionStatus(solrHistorianConf.chunkCollection);
            final NamedList<Object> rsp = solrHistorianConf.client.request(request);
            final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
            int status = (int) responseHeader.get("status");
            return status;
        } catch (IOException | SolrServerException e) {
            throw e;
        } catch (SolrException e) {
            LOGGER.warn("Could not connect so solr");
            if (numberOfRetry == 0)
                throw e;
            LOGGER.info("waiting {} ms before retrying.", sleepDurationMilli);
            try {
                Thread.sleep(sleepDurationMilli);
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException exception", e);
                throw e;
            }
            int triesLeft = numberOfRetry - 1;
            LOGGER.info("Retrying to connect to solr, {} {} left.", triesLeft, triesLeft == 1 ? "try" : "tries");
            return pingSolrServer(sleepDurationMilli, triesLeft);
        }
    }

    @Override
    public HistorianService getTimeSeriesChunk(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        final SolrQuery query = buildTimeSeriesChunkQuery(params);
        query.setFields();//so we return every fields (this endpoint is currently used only in tests, this is legacy code)
        //  EXECUTE REQUEST
        Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, query);
                final SolrDocumentList documents = response.getResults();
                LOGGER.debug("Found " + documents.getNumFound() + " documents");
                JsonArray docs = new JsonArray(documents.stream()
                        .map(this::convertDoc)
                        .collect(Collectors.toList())
                );
                p.complete(new JsonObject()
                        .put(HistorianFields.TOTAL, documents.getNumFound())
                        .put(CHUNKS, docs)
                );
            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
        vertx.executeBlocking(getTimeSeriesHandler, resultHandler);
        return this;
    }

    private SolrQuery buildTimeSeriesChunkQuery(JsonObject params) {
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
        //    FILTER
//        buildSolrFilterFromArray(params.getJsonArray(HistorianFields.TAGS), RESPONSE_TAG_NAME_FIELD)
//                .ifPresent(query::addFilterQuery);
        buildSolrFilterFromTags(params.getJsonObject(HistorianFields.TAGS))
                .ifPresent(query::addFilterQuery);
        buildSolrFilterFromArray(params.getJsonArray(NAMES), NAME)
                .ifPresent(query::addFilterQuery);
        //    FIELDS_TO_FETCH
        query.setFields(RESPONSE_CHUNK_START_FIELD,
                RESPONSE_CHUNK_END_FIELD,
                RESPONSE_CHUNK_SIZE_FIELD,
                NAME);
        //    SORT
        query.setSort(RESPONSE_CHUNK_START_FIELD, SolrQuery.ORDER.asc);
        query.addSort(RESPONSE_CHUNK_END_FIELD, SolrQuery.ORDER.asc);
        query.setRows(params.getInteger(MAX_TOTAL_CHUNKS_TO_RETRIEVE, 50000));
        return query;
    }
    private SolrQuery buildAnnotationQuery(JsonObject params) {
        StringBuilder queryBuilder = new StringBuilder();
        Long from = params.getLong(FROM);
        Long to = params.getLong(TO);
        if (to == null && from != null) {
            LOGGER.trace("requesting annotation with from time {}", from);
            queryBuilder.append(TIME).append(":[").append(from).append(" TO ").append("*]");
        } else if (from == null && to != null) {
            LOGGER.trace("requesting annotation with to time {}", to);
            queryBuilder.append(TIME).append(":[*").append(" TO ").append(to).append("]");
        } else if (from != null) {
            LOGGER.trace("requesting annotation with time from {} to time {}", from, to);
            queryBuilder.append(TIME).append(":[").append(from).append(" TO ").append(to).append("]");
        } else {
            LOGGER.trace("requesting annotation with all times existing");
            queryBuilder.append("*:*");
        }
        //FILTER
        List<String> tags = null;
        if (params.getJsonArray(TAGS) != null)
            tags = params.getJsonArray(TAGS).getList();
        StringBuilder stringQuery = new StringBuilder();
        String operator = "";
        SolrQuery query = new SolrQuery();
        switch (AnnotationRequestType.valueOf(params.getString(TYPE, AnnotationRequestType.ALL.toString()))) {
            case ALL:
                break;
            case TAGS:
                queryBuilder.append(" && ");
                if (!params.getBoolean(MATCH_ANY, true)) {
                    operator = " AND ";
                } else {
                    operator = " OR ";
                }
                for (String tag : tags.subList(0,tags.size()-1)) {
                    stringQuery.append(tag).append(operator);
                }
                stringQuery.append(tags.get(tags.size()-1));
                queryBuilder.append(HistorianFields.TAGS).append(":").append("(").append(stringQuery.toString()).append(")");
                break;
        }
        if (queryBuilder.length() != 0 ) {
            LOGGER.info("query is : {}", queryBuilder.toString());
            query.setQuery(queryBuilder.toString());
        }
        query.setRows(params.getInteger(LIMIT, 1000));
        //    FIELDS_TO_FETCH
        query.setFields(TIME,
                TIME_END_REQUEST_FIELD,
                TEXT,
                TAGS);
        query.addSort("score", SolrQuery.ORDER.desc);
        query.addSort(TIME, SolrQuery.ORDER.desc);
        return query;
    }

    private Optional<String> buildSolrFilterFromArray(JsonArray jsonArray, String responseMetricNameField) {
        if (jsonArray == null || jsonArray.isEmpty())
            return Optional.empty();
        if (jsonArray.size() == 1) {
            return Optional.of(responseMetricNameField + ":\"" + jsonArray.getString(0) + "\"");
        } else {
            String orNames = jsonArray.stream()
                    .map(String.class::cast)
                    .collect(Collectors.joining("\" OR \"", "(\"", "\")"));
            return Optional.of(responseMetricNameField + ":" + orNames);
        }
    }

    private Optional<String> buildSolrFilterFromTags(JsonObject tags) {
        if (tags == null || tags.isEmpty())
            return Optional.empty();
        String filters = tags.fieldNames().stream()
                .map(tagName -> {
                    String value = tags.getString(tagName);
                    return tagName + ":\"" + value + "\"";
                })
                .collect(Collectors.joining(" OR ", "", ""));
        return Optional.of(filters);
    }

    @Override
    public HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        String name = params.getString(HistorianFields.METRIC);
        String queryString = NAME +":*";
        if (name!=null && !name.isEmpty()) {
            queryString = NAME + ":*" + name + "*";
        }
        SolrQuery query = new SolrQuery(queryString);
        query.setFilterQueries(queryString);
        int max = params.getInteger(LIMIT, solrHistorianConf.maxNumberOfTargetReturned);
        query.setRows(0);//we only need distinct values of metrics
        query.setFacet(true);
//        query.setFacetSort("index");
//        query.setFacetPrefix("per");
        query.setFacetLimit(max);
        query.setFacetMinCount(1);//number of doc matching the query is at least 1
        query.addFacetField(NAME);
        //  EXECUTE REQUEST
        Handler<Promise<JsonObject>> getMetricsNameHandler = p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.chunkCollection, query);
                FacetField facetField = response.getFacetField(NAME);
                List<FacetField.Count> facetFieldsCount = facetField.getValues();
                if (facetFieldsCount.size() == 0) {
                    p.complete(new JsonObject()
                            .put(TOTAL, 0)
                            .put(METRICS, new JsonArray())
                    );
                    return;
                }
                LOGGER.debug("Found " + facetField.getValueCount() + " different values");
                JsonArray metrics = new JsonArray(facetFieldsCount.stream()
                        .map(FacetField.Count::getName)
                        .collect(Collectors.toList())
                );
                p.complete(new JsonObject()
                        .put(TOTAL, facetField.getValueCount())
                        .put(METRICS, metrics)
                );
            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
        vertx.executeBlocking(getMetricsNameHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService getAnnotations(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        final SolrQuery query = buildAnnotationQuery(params);
        Handler<Promise<JsonObject>> getAnnoationsHandler = p -> {
            try {
                final QueryResponse response = solrHistorianConf.client.query(solrHistorianConf.annotationCollection, query);
                SolrDocumentList solrDocuments = response.getResults();
                LOGGER.debug("Found " + response.getRequestUrl() + response + " result" + solrDocuments);
                JsonArray annotation = new JsonArray(new ArrayList<>(solrDocuments)
                );
                LOGGER.debug("annotations found : "+ annotation);
                p.complete(new JsonObject()
                        .put(ANNOTATIONS, annotation)
                        .put(HistorianFields.TOTAL_HIT, annotation.size())
                );
            } catch (IOException | SolrServerException e) {
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
        vertx.executeBlocking(getAnnoationsHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService addTimeSeries(JsonObject timeseriesObject, Handler<AsyncResult<JsonObject>> resultHandler) {
     Handler<Promise<JsonObject>> getMetricsNameHandler = p -> {
            try {
                final String chunkOrigin = null;//TODO
                JsonArray timeseriesPoints = timeseriesObject.getJsonArray("correctPoints");
                JsonObject response = new JsonObject();
                List<Map<String, Object>> resultForCsvImport = new LinkedList<>();
                int numChunk = 0;
                int numPoints = 0;
                for(Object timeseriesPointsObject : timeseriesPoints) {
                    try {
                        JsonArray timeseries =(JsonArray) timeseriesPointsObject;
                        Collection<SolrInputDocument> documents = new ArrayList<>();
                        JsonArray groupedByFields = timeseriesObject.getJsonArray(GROUPED_BY); // to catch the group by fields
                        List<HashMap<String, String>> groupedByFieldsForEveryChunk = new LinkedList<>();
                        for (Object timeserieObject : timeseries) {
                            JsonObject timeserie = (JsonObject) timeserieObject;
                            JsonObject tagsObject = timeserie.getJsonObject(TAGS); // here we get the tags object tso we ( with the list of groupby ) get the values of the group by !!
                            SolrInputDocument document;
                            LOGGER.info("building SolrDocument from a chunk");
                            document = chunkTimeSerie(timeserie, chunkOrigin);
                            documents.add(document);
                            HashMap<String, String> groupedByFieldsForThisChunk = new LinkedHashMap<String,String>();
                            groupedByFields.forEach(f -> {
                                if (f.toString().equals(NAME))
                                    groupedByFieldsForThisChunk.put(f.toString(),timeserie.getString(f.toString()));
                                else
                                    groupedByFieldsForThisChunk.put(f.toString(),tagsObject.getString(f.toString()));
                            });
                            int totalNumPointsInChunk = (int) document.getFieldValue(RESPONSE_CHUNK_SIZE_FIELD);
                            numChunk++;
                            numPoints = numPoints + totalNumPointsInChunk;
                            groupedByFieldsForThisChunk.put("totalPointsForThisChunk", String.valueOf(totalNumPointsInChunk));
                            groupedByFieldsForEveryChunk.add(groupedByFieldsForThisChunk);
                        }
                        if(!documents.isEmpty()) {
                            LOGGER.info("adding some chunks in collection {}", solrHistorianConf.chunkCollection);
                            solrHistorianConf.client.add(solrHistorianConf.chunkCollection, documents);
                            solrHistorianConf.client.commit(solrHistorianConf.chunkCollection);
                            LOGGER.info("added with success some chunks in collection {}", solrHistorianConf.chunkCollection);
                        }
                        resultForCsvImport.addAll(prepareTheResultForCsv(groupedByFieldsForEveryChunk, groupedByFields));
                    } catch (SolrServerException | IOException e) {
                        e.printStackTrace();
                    }
                }
                /*if(timeseriesObject.getValue(IMPORT_TYPE) == ImportRequestType.CSV.toString())
                    response.put(CSV,new JsonArray(resultForCsvImport));
                if(timeseriesObject.getValue(IMPORT_TYPE) == ImportRequestType.JSON.toString())
                    response.put(RESPONSE_TOTAL_ADDED_POINTS, numPoints).put(RESPONSE_TOTAL_ADDED_CHUNKS, numChunk);
                p.complete(response
                );*/
            } catch (Exception e) {
                LOGGER.error("unexpected exception");
                p.fail(e);
            }
        };
        vertx.executeBlocking(getMetricsNameHandler, resultHandler);

        return this;
    }

    List<Map<String, Object>> prepareTheResultForCsv(List<HashMap<String, String>> groupedByFieldsForEveryChunk, JsonArray groupBdByFields) {
        return groupedByFieldsForEveryChunk.stream()
                .collect(Collectors.groupingBy(map -> {
                            HashMap<String, String> groupedByFieldsForThisMap = new LinkedHashMap<String,String>();
                            groupBdByFields.forEach(f -> groupedByFieldsForThisMap.put(f.toString(), map.get(f)));
                            return groupedByFieldsForThisMap;
                        },LinkedHashMap::new,
                        Collectors.mapping(map -> map.get("totalPointsForThisChunk"),
                                Collectors.toList()))).entrySet().stream().map(entry -> {
                    Map<String, Object> resultObject = new LinkedHashMap<>();
                    int totalNumberOfPointsPerGroupedFildes = 0;
                    int chunkNumber = entry.getValue().size();
                    entry.getKey().forEach(resultObject::put);
                    totalNumberOfPointsPerGroupedFildes = entry.getValue().stream().mapToInt(Integer::valueOf).sum();
                    resultObject.put("number_of_points_injected", totalNumberOfPointsPerGroupedFildes);
                    resultObject.put("number_of_point_failed", 0); // WIP here !
                    resultObject.put("number_of_chunk_created", chunkNumber);
                    return resultObject;
                }).collect(Collectors.toList());
    }

    private SolrInputDocument chunkTimeSerie(JsonObject timeserie, String chunkOrigin) {
        JsonObjectToChunk jsonObjectToChunk = new JsonObjectToChunk(chunkOrigin);
        SolrInputDocument doc = jsonObjectToChunk.chunkIntoSolrDocument(timeserie);
        return doc;
    }

    /**
     * nombre point < LIMIT_TO_DEFINE ==> Extract points from chunk
     * nombre point >= LIMIT_TO_DEFINE && nombre de chunk < LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg)
     * nombre de chunk >= LIMIT_TO_DEFINE ==> Sample points with chunk aggs depending on alg (min, avg),
     * but should using agg on solr side (using key partition, by month, daily ? yearly ?)
     */
    @Override
    public HistorianService getTimeSeries(JsonObject myParams, Handler<AsyncResult<JsonObject>> myResult) {
        final SolrQuery query = buildTimeSeriesChunkQuery(myParams);
        Handler<Promise<JsonObject>> getTimeSeriesHandler = p -> {
            MetricsSizeInfo metricsInfo;
            try {
                metricsInfo = getNumberOfPointsByMetricInRequest(query);
                LOGGER.debug("metrics info to query : {}", metricsInfo);
                if (metricsInfo.isEmpty()) {
                    final MultiTimeSeriesExtracter timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(myParams, metricsInfo);
                    p.complete(buildTimeSeriesResponse(timeSeriesExtracter));
                    return;
                }
                final MultiTimeSeriesExtracter timeSeriesExtracter = getMultiTimeSeriesExtracter(myParams, query, metricsInfo);
                requestSolrAndBuildTimeSeries(query, p, timeSeriesExtracter);
            } catch (IOException e) {
                LOGGER.error("unexpected io exception", e);
                p.fail(e);
            } catch (Exception e) {
                LOGGER.error("unexpected exception", e);
                p.fail(e);
            }
        };
        vertx.executeBlocking(getTimeSeriesHandler, myResult);
        return this;
    }

    public MultiTimeSeriesExtracter getMultiTimeSeriesExtracter(JsonObject myParams, SolrQuery query, MetricsSizeInfo metricsInfo) {
        //TODO make three different group for each metrics, not use a single strategy globally for all metrics.
        final MultiTimeSeriesExtracter timeSeriesExtracter;
        if (metricsInfo.getTotalNumberOfPoints() < solrHistorianConf.limitNumberOfPoint ||
                metricsInfo.getTotalNumberOfPoints() <= getSamplingConf(myParams).getMaxPoint()) {
            LOGGER.debug("QUERY MODE 1: metricsInfo.getTotalNumberOfPoints() < limitNumberOfPoint");
            query.addField(RESPONSE_CHUNK_VALUE_FIELD);
            timeSeriesExtracter = createTimeSerieExtractorSamplingAllPoints(myParams, metricsInfo);
        } else if (metricsInfo.getTotalNumberOfChunks() < solrHistorianConf.limitNumberOfChunks) {
            LOGGER.debug("QUERY MODE 2: metricsInfo.getTotalNumberOfChunks() < limitNumberOfChunks");
            addFieldsThatWillBeNeededBySamplingAlgorithms(myParams, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(myParams, metricsInfo);
        } else {
            LOGGER.debug("QUERY MODE 3 : else");
            //TODO Sample points with chunk aggs depending on alg (min, avg),
            // but should using agg on solr side (using key partition, by month, daily ? yearly ?)
            // For the moment we use the stream api without partitionning
            addFieldsThatWillBeNeededBySamplingAlgorithms(myParams, query, metricsInfo);
            timeSeriesExtracter = createTimeSerieExtractorUsingChunks(myParams, metricsInfo);
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
                case FIRST_ITEM:
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
    private MultiTimeSeriesExtracter createTimeSerieExtractorUsingChunks(JsonObject params, MetricsSizeInfo metricsInfo) {
        long from = params.getLong(FROM);
        long to = params.getLong(TO);
        SamplingConf requestedSamplingConf = getSamplingConf(params);
        MultiTimeSeriesExtractorUsingPreAgg timeSeriesExtracter = new MultiTimeSeriesExtractorUsingPreAgg(from, to, requestedSamplingConf);
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        return timeSeriesExtracter;
    }

    //TODO from, to and SamplingConf as parameter. So calcul SampligConf before this method not in MultiTimeSeriesExtracterImpl
    private MultiTimeSeriesExtracter createTimeSerieExtractorSamplingAllPoints(JsonObject params, MetricsSizeInfo metricsInfo) {
        long from = params.getLong(FROM);
        long to = params.getLong(TO);
        SamplingConf requestedSamplingConf = getSamplingConf(params);
        MultiTimeSeriesExtracterImpl timeSeriesExtracter = new MultiTimeSeriesExtracterImpl(from, to, requestedSamplingConf);
        fillingExtractorWithMetricsSizeInfo(timeSeriesExtracter, metricsInfo);
        return timeSeriesExtracter;
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


//    private MetricSizeInfo getNumberOfPointsInByRequest(SolrQuery query) throws IOException {//TODO better handling of exception
//        String cexpr = String.format("stats(%s,\n" +
//                "q=\"%s\",\n" +
//                "sum(chunk_size), count(*))",
//                collection, query.getQuery());
//        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
//        paramsLoc.set("expr", cexpr);
//        paramsLoc.set("qt", "/stream");
//        TupleStream solrStream = new SolrStream(streamEndPoint, paramsLoc);
//        StreamContext context = new StreamContext();
//        solrStream.setStreamContext(context);
//        solrStream.open();
//        Tuple tuple = solrStream.read();
//        long numberOfChunk = -1;
//        long numberOfPoints = -1;
//        while (!tuple.EOF) {
//            LOGGER.trace("tuple : {}", tuple.jsonStr());
//            numberOfPoints = tuple.getLong("sum(chunk_size)");
//            numberOfChunk = tuple.getLong("count(*)");
//            tuple = solrStream.read();
//        }
//        LOGGER.debug("metric response : {}", tuple.jsonStr());
//        solrStream.close(); //TODO could be try-with-resources
//        MetricSizeInfo metrics = new MetricSizeInfo();
//        metrics.totalNumberOfChunks = numberOfChunk;
//        metrics.totalNumberOfPoints = numberOfPoints;
//        return metrics;
//    }

    private MetricsSizeInfo getNumberOfPointsByMetricInRequest(SolrQuery query) throws IOException {//TODO better handling of exception
//        String cexpr = "rollup(search(historian, q=\"*:*\", fl=\"chunk_size, name\", qt=\"/export\", sort=\"name asc\"),\n" +
//                "\t\t\t\t over=\"name\", sum(chunk_size))";
        StringBuilder exprBuilder = new StringBuilder("rollup(search(").append(solrHistorianConf.chunkCollection)
                .append(",q=\"").append(query.getQuery()).append("\"");
        if (query.getFilterQueries() != null) {
            for (String filterQuery : query.getFilterQueries()) {
                exprBuilder
                        .append(",fq=\"").append(filterQuery).append("\"");
            }
        }
        exprBuilder.append(",fl=\"").append(RESPONSE_CHUNK_SIZE_FIELD).append(", ")
                .append(NAME).append("\"")
                .append(",qt=\"/export\", sort=\"").append(NAME).append(" asc\")")
                .append(",over=\"name\", sum(chunk_size), count(*))");
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
            metric.totalNumberOfPoints = tuple.getLong("sum(chunk_size)");
            metricsInfo.setMetricInfo(metric);
            tuple = solrStream.read();
        }
        LOGGER.debug("metric response : {}", tuple.jsonStr());
        solrStream.close(); //TODO could be try-with-resources
        return metricsInfo;
    }

    private JsonObject convertDoc(SolrDocument doc) {
        final JsonObject json = new JsonObject();
        doc.getFieldNames().forEach(f -> {
            json.put(f, doc.get(f));
        });
        return json;
    }
}
