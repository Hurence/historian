package com.hurence.webapiservice.historian;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianAddVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = "historian";

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.initHistorianSolr(client);
        HistorianSolrITHelper
                .deployHistorienVerticle(container, vertx)
                .subscribe(id -> {
                            historian = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), "historian_service");
                            context.completeNow();
                        },
                        t -> context.failNow(t));

    }

    @AfterAll
    static void finish(SolrClient client, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        LOGGER.debug("deleting collection {}", COLLECTION);
        final SolrRequest deleteRequest = CollectionAdminRequest.deleteCollection(COLLECTION);
        client.request(deleteRequest);
        LOGGER.debug("closing vertx");
        vertx.close(context.completing());
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void addTimeseries(VertxTestContext testContext) {
        long time1 = 1477895624866L;
        long time2 = 1477916224866L;
        JsonArray params = new JsonArray().add(new JsonObject().put("name", "openSpaceSensors.Temperature111").put("points", new JsonArray().add(new JsonArray().add(time1).add(2.0)).add(new JsonArray().add(time2).add(4.0))));
        historian.rxAddTimeSeries(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(rsp, new JsonObject("{}"));
                    });
                })
                .subscribe();
        JsonObject params1 = new JsonObject();
        historian.rxGetTimeSeriesChunk(params1)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        LOGGER.info("responces : {}", rsp);
                        long totalHit = rsp.getLong(RESPONSE_TOTAL_FOUND);
                        assertEquals(1, totalHit);
                        JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                        assertEquals(1, docs.size());
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertTrue(doc1.containsKey(RESPONSE_METRIC_NAME_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_START_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_END_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_ID_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SIZE_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_VALUE_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_WINDOW_MS_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SIZE_BYTES_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_VERSION_FIELD));
                        assertEquals(time1, doc1.getLong(RESPONSE_CHUNK_START_FIELD));
                        assertEquals(time2, doc1.getLong(RESPONSE_CHUNK_END_FIELD));
                        testContext.completeNow();
                    });
                })
                .subscribe();

    }


}

