package com.hurence.webapiservice.historian.impl;

import com.hurence.historian.modele.Schema;
import com.hurence.webapiservice.historian.HistorianService;
import com.hurence.webapiservice.historian.handler.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Handler<Promise<Integer>> pingHandler = createPingHandler(solrHistorianConf.sleepDurationBetweenTry, solrHistorianConf.numberOfRetryToConnect);
        Handler<Promise<Optional<String>>> checkSchemaHandler = createCheckSchemaHandler();
        Handler<AsyncResult<Optional<String>>> historianStatusHandler = h -> {
            if (h.succeeded()) {
                if (!h.result().isPresent()) {
                    LOGGER.debug("historianStatusHandler");
                    readyHandler.handle(Future.succeededFuture(this));
                } else {
                    readyHandler.handle(Future.failedFuture(new IllegalArgumentException(
                            String.format("historian chunk schema collection '%s' is not as expected :\n%s",
                                    solrHistorianConf.chunkCollection,
                                    h.result().get())
                    )));
                }
            } else {
                readyHandler.handle(Future.failedFuture(h.cause()));
            }
        };
        Handler<AsyncResult<Integer>> pingResponseHandler = h -> {
            if (h.succeeded()) {
                if (h.result() == 0) {
                    LOGGER.debug("pingResponseHandler");
                    vertx.executeBlocking(checkSchemaHandler, historianStatusHandler);
                } else {
                    readyHandler.handle(Future.failedFuture(new IllegalArgumentException(
                            String.format("historian collection ping command returned status %d", h.result())
                    )));
                }
            } else {
                readyHandler.handle(Future.failedFuture(h.cause()));
            }
        };
        vertx.executeBlocking(pingHandler, pingResponseHandler);
    }

    private Handler<Promise<Optional<String>>> createCheckSchemaHandler() {
        return p -> {
            try {
                p.complete(checkSchema());
            } catch (IOException e) {
                LOGGER.error("IOException while pinging solr", e);
                p.fail(e);
            } catch (SolrServerException e) {
                LOGGER.error("SolrServerException while pinging solr", e);
                p.fail(e);
            }
        };
    }

    private Optional<String> checkSchema() throws IOException, SolrServerException {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(solrHistorianConf.client, solrHistorianConf.chunkCollection);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
        Map<String, Map<String, Object>> schemaGroupedByName = schema.stream()
                .collect(
                        Collectors.toMap(
                                map -> {
                                    return (String) map.get("name");
                                },
                                map -> {
                                    return map;
                                }

                        ));
        Schema expectedSchema = Schema.getChunkSchema(solrHistorianConf.schemaVersion);
        List<String> errors = expectedSchema.getFields().stream().flatMap(f -> {
            if (!schemaGroupedByName.containsKey(f.getName())) {
                return Stream.of(String.format(
                        "* schema does not contain field '%s'", f
                ));
            }
            Map<String, Object> fieldConf = schemaGroupedByName.get(f.getName());
            String errMsg = null;
            if (fieldConf.get("type") != null && !fieldConf.get("type").equals(f.getType())) {
                errMsg = String.format(
                        "* field '%s' type is %s instead of '%s'", f.getName(), fieldConf.get("type"), f.getType());
            }
            if (fieldConf.get("multiValued") != null && !fieldConf.get("multiValued").equals(f.getMultivalued())) {
                errMsg += String.format(
                        "\n* field '%s' has multivalued set to %s instead of '%s'", f.getName(), fieldConf.get("multiValued"), f.getMultivalued());
            }
            if (fieldConf.get("indexed") != null && !fieldConf.get("indexed").equals(f.getIndexed())) {
                errMsg += String.format(
                        "\n* field '%s' has indexed set to %s instead of '%s'", f.getName(), fieldConf.get("indexed"), f.getIndexed());
            }
            if (fieldConf.get("required") != null && !fieldConf.get("required").equals(f.getRequired())) {
                errMsg += String.format(
                        "\n* field '%s' has required set to %s instead of '%s'", f.getName(), fieldConf.get("required"), f.getRequired());
            }
            if (fieldConf.get("stored") != null && !fieldConf.get("stored").equals(f.getStored())) {
                errMsg += String.format(
                        "\n* field '%s' has stored set to %s instead of '%s'", f.getName(), fieldConf.get("stored"), f.getStored());
            }
            if (errMsg != null) {
                return Stream.of(errMsg);
            }
            return Stream.empty();
        }).collect(Collectors.toList());
        LOGGER.debug(solrHistorianConf.chunkCollection + " schema is {}", new JsonArray(schema).encodePrettily());
        if (errors.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                errors.stream().collect(Collectors.joining("\n\n"))
        );
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
        GetTimeSeriesChunkHandler requestHandler = new GetTimeSeriesChunkHandler(solrHistorianConf);
        Handler<Promise<JsonObject>> getTimeSeriesChunkHandler = requestHandler.getHandler(params);
        vertx.executeBlocking(getTimeSeriesChunkHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService getMetricsName(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        GetMetricsNameHandler requestHandler = new GetMetricsNameHandler(solrHistorianConf);
        Handler<Promise<JsonObject>> getMetricsNameHandler = requestHandler.getHandler(params);
        vertx.executeBlocking(getMetricsNameHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService getAnnotations(JsonObject params, Handler<AsyncResult<JsonObject>> resultHandler) {
        GetAnnotationsHandler requestHandler = new GetAnnotationsHandler(solrHistorianConf);
        Handler<Promise<JsonObject>> annotationHandler = requestHandler.getHandler(params);
        vertx.executeBlocking(annotationHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService addTimeSeries(JsonObject timeseriesObject, Handler<AsyncResult<JsonObject>> resultHandler) {
        AddTimeSeriesHandler requestHandler = new AddTimeSeriesHandler(solrHistorianConf);
        Handler<Promise<JsonObject>> addHandler = requestHandler.getHandler(timeseriesObject);
        vertx.executeBlocking(addHandler, resultHandler);
        return this;
    }

    @Override
    public HistorianService getTimeSeries(JsonObject myParams, Handler<AsyncResult<JsonObject>> myResult) {
        GetTimeSeriesHandler requestHandler = new GetTimeSeriesHandler(solrHistorianConf);
        Handler<Promise<JsonObject>>  getTimeSeriesHandler = requestHandler.getTimeSeriesHandler(myParams);
        vertx.executeBlocking(getTimeSeriesHandler, myResult);
        return this;
    }

}
