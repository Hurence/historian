package com.hurence.webapiservice.historian;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hurence.historian.modele.HistorianFields.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianImportJsonVerticleIT {

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
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void importJsonTimeseries(VertxTestContext testContext) {
        long time1 = 1477895624866L;
        long time2 = 1477916224866L;
        JsonArray params = new JsonArray().add(new JsonObject().put("name", "openSpaceSensors.Temperature000").put("points", new JsonArray().add(new JsonArray().add(time1).add(2.0)).add(new JsonArray().add(time2).add(4.0))));
        historian.rxAddTimeSeries(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject jsonObject = new JsonObject().put("status", "OK").put("message", "injected 1 chunks");
                        assertEquals(rsp, jsonObject);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject("{\"from\":1477895614866," +
                            "\"to\": 1477916925845," +
                            "\"fields\":[\"chunk_value\",\"chunk_start\",\"chunk_end\",\"chunk_size\",\"name\"]," +
                            "\"names\":[\"openSpaceSensors.Temperature000\"]," +
                            "\"tags\":[]," +
                            "\"sampling_algo\":\"AVERAGE\"," +
                            "\"bucket_size\":1," +
                            "\"max_points_to_return_by_metric\":844" +
                            "}");
                    historian.rxGetTimeSeries(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responces : {}", rsp);
                                    long totalPoints = rsp.getLong(TOTAL_POINTS_RESPONSE_FIELD);
                                    assertEquals(2, totalPoints);
                                    JsonArray docs = rsp.getJsonArray(TIMESERIES_RESPONSE_FIELD);
                                    assertEquals(1, docs.size());
                                    JsonObject doc1 = docs.getJsonObject(0);
                                    assertEquals("openSpaceSensors.Temperature000", doc1.getString(TARGET_RESPONSE_FIELD));
                                    JsonArray datapoints1 = doc1.getJsonArray(DATAPOINTS_RESPONSE_FIELD);
                                    assertEquals(new JsonArray("[[2.0,1477895624866],[4.0,1477916224866]]"), datapoints1);
                                    testContext.completeNow();
                                });
                            })
                            .subscribe();
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    void importMultiJsonTimeseries(VertxTestContext testContext) {
        long time1 = 1477895624866L;
        long time2 = 1477916224866L;
        long time3 = 1477895724888L;
        long time4 = 1477916924845L;
        JsonArray params = new JsonArray().add(new JsonObject().put("name", "openSpaceSensors.Temperature111").put("points", new JsonArray().add(new JsonArray().add(time1).add(2.0)).add(new JsonArray().add(time2).add(4.0))))
                .add(new JsonObject().put("name", "openSpaceSensors.Temperature222").put("points", new JsonArray().add(new JsonArray().add(time1).add(3.1)).add(new JsonArray().add(time2).add(8.8))))
                .add(new JsonObject().put("name", "openSpaceSensors.Temperature333").put("points", new JsonArray().add(new JsonArray().add(time3).add(4.1)).add(new JsonArray().add(time4).add(6.5))))
                .add(new JsonObject().put("name", "openSpaceSensors.Temperature444").put("points", new JsonArray().add(new JsonArray().add(time3).add(0.0)).add(new JsonArray().add(time4).add(9.1))));
        historian.rxAddTimeSeries(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject jsonObject = new JsonObject().put("status", "OK").put("message", "injected 4 chunks");
                        assertEquals(rsp, jsonObject);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject("{\"from\":1477895614866," +
                            "\"to\": 1477916925845," +
                            "\"fields\":[\"chunk_value\",\"chunk_start\",\"chunk_end\",\"chunk_size\",\"name\"]," +
                            "\"names\":[\"openSpaceSensors.Temperature111\",\"openSpaceSensors.Temperature222\"," +
                            "\"openSpaceSensors.Temperature333\"," +
                            "\"openSpaceSensors.Temperature444\"]," +
                            "\"tags\":[]," +
                            "\"sampling_algo\":\"AVERAGE\"," +
                            "\"bucket_size\":1," +
                            "\"max_points_to_return_by_metric\":844" +
                            "}");
                    historian.rxGetTimeSeries(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responces : {}", rsp);
                                    long totalPoints = rsp.getLong(TOTAL_POINTS_RESPONSE_FIELD);
                                    assertEquals(8, totalPoints);
                                    JsonArray docs = rsp.getJsonArray(TIMESERIES_RESPONSE_FIELD);
                                    assertEquals(4, docs.size());
                                    JsonObject doc1 = docs.getJsonObject(3);
                                    assertEquals("openSpaceSensors.Temperature111", doc1.getString(TARGET_RESPONSE_FIELD));
                                    JsonArray datapoints1 = doc1.getJsonArray(DATAPOINTS_RESPONSE_FIELD);
                                    assertEquals(new JsonArray("[[2.0,1477895624866],[4.0,1477916224866]]"), datapoints1);
                                    JsonObject doc2 = docs.getJsonObject(2);
                                    assertEquals("openSpaceSensors.Temperature222", doc2.getString(TARGET_RESPONSE_FIELD));
                                    JsonArray datapoints2 = doc2.getJsonArray(DATAPOINTS_RESPONSE_FIELD);
                                    assertEquals(new JsonArray("[[3.1,1477895624866],[8.8,1477916224866]]"), datapoints2);
                                    JsonObject doc3 = docs.getJsonObject(1);
                                    assertEquals("openSpaceSensors.Temperature333", doc3.getString(TARGET_RESPONSE_FIELD));
                                    JsonArray datapoints3 = doc3.getJsonArray(DATAPOINTS_RESPONSE_FIELD);
                                    assertEquals(new JsonArray("[[4.1,1477895724888],[6.5,1477916924845]]"), datapoints3);
                                    JsonObject doc4 = docs.getJsonObject(0);
                                    assertEquals("openSpaceSensors.Temperature444", doc4.getString(TARGET_RESPONSE_FIELD));
                                    JsonArray datapoints4 = doc4.getJsonArray(DATAPOINTS_RESPONSE_FIELD);
                                    assertEquals(new JsonArray("[[0.0,1477895724888],[9.1,1477916924845]]"), datapoints4);
                                    testContext.completeNow();
                                });
                            })
                            .subscribe();
                })
                .subscribe();
    }
}
