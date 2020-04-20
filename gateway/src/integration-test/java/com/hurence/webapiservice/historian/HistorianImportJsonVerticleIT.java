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
        JsonArray params = new JsonArray().add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature000").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time1).add(2.0)).add(new JsonArray().add(time2).add(4.0))));
        historian.rxAddTimeSeries(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject expectedResponse = new JsonObject().put(RESPONSE_TOTAL_ADDED_POINTS, 2).put(RESPONSE_TOTAL_ADDED_CHUNKS, 1);
                        assertEquals(rsp, expectedResponse);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject("{\""+FROM_REQUEST_FIELD+"\":1477895614866," +
                            "\""+TO_REQUEST_FIELD+"\": 1477916925845," +
                            "\""+FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD+"\":[\""+RESPONSE_CHUNK_VALUE_FIELD+"\",\""+RESPONSE_CHUNK_START_FIELD+"\",\""+RESPONSE_CHUNK_END_FIELD+"\",\""+RESPONSE_CHUNK_SIZE_FIELD+"\",\""+RESPONSE_METRIC_NAME_FIELD+"\"]," +
                            "\""+METRIC_NAMES_AS_LIST_REQUEST_FIELD+"\":[\"openSpaceSensors.Temperature000\"]," +
                            "\""+TAGS_TO_FILTER_ON_REQUEST_FIELD+"\":[]," +
                            "\""+SAMPLING_ALGO_REQUEST_FIELD+"\":\"AVERAGE\"," +
                            "\""+BUCKET_SIZE_REQUEST_FIELD+"\":1," +
                            "\""+MAX_POINT_BY_METRIC_REQUEST_FIELD+"\":844" +
                            "}");
                    historian.rxGetTimeSeries(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responses : {}", rsp);
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
        JsonArray params = new JsonArray().add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature111").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time1).add(2.0)).add(new JsonArray().add(time2).add(4.0))))
                .add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature222").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time1).add(3.1)).add(new JsonArray().add(time2).add(8.8))))
                .add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature333").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time3).add(4.1)).add(new JsonArray().add(time4).add(6.5))))
                .add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature444").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time3).add(0.0)).add(new JsonArray().add(time4).add(9.1))));
        historian.rxAddTimeSeries(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject expectedResponse = new JsonObject().put(RESPONSE_TOTAL_ADDED_POINTS, 8).put(RESPONSE_TOTAL_ADDED_CHUNKS, 4);
                        assertEquals(rsp, expectedResponse);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject("{\""+FROM_REQUEST_FIELD+"\":1477895614866," +
                            "\""+TO_REQUEST_FIELD+"\": 1477916925845," +
                            "\""+FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD+"\":[\""+RESPONSE_CHUNK_VALUE_FIELD+"\",\""+RESPONSE_CHUNK_START_FIELD+"\",\""+RESPONSE_CHUNK_END_FIELD+"\",\""+RESPONSE_CHUNK_SIZE_FIELD+"\",\""+RESPONSE_METRIC_NAME_FIELD+"\"]," +
                            "\""+METRIC_NAMES_AS_LIST_REQUEST_FIELD+"\":[\"openSpaceSensors.Temperature111\",\"openSpaceSensors.Temperature222\"," +
                            "\"openSpaceSensors.Temperature333\"," +
                            "\"openSpaceSensors.Temperature444\"]," +
                            "\""+TAGS_TO_FILTER_ON_REQUEST_FIELD+"\":[]," +
                            "\""+SAMPLING_ALGO_REQUEST_FIELD+"\":\"AVERAGE\"," +
                            "\""+BUCKET_SIZE_REQUEST_FIELD+"\":1," +
                            "\""+MAX_POINT_BY_METRIC_REQUEST_FIELD+"\":844" +
                            "}");
                    historian.rxGetTimeSeries(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responses : {}", rsp);
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

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void checkAddedTimeseriesChunks(VertxTestContext testContext) {
        long time1 = 1477895624866L;
        long time2 = 1477916224866L;
        JsonArray params = new JsonArray().add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature555").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time1).add(2.0)).add(new JsonArray().add(time2).add(4.0))))
        .add(new JsonObject().put(METRIC_NAME_REQUEST_FIELD, "openSpaceSensors.Temperature666").put(POINTS_REQUEST_FIELD, new JsonArray().add(new JsonArray().add(time1).add(3.1)).add(new JsonArray().add(time2).add(8.8))));
        historian.rxAddTimeSeries(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject expectedResponse = new JsonObject().put(RESPONSE_TOTAL_ADDED_POINTS, 4).put(RESPONSE_TOTAL_ADDED_CHUNKS, 2);
                        assertEquals(rsp, expectedResponse);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject().put("names", new JsonArray().add("openSpaceSensors.Temperature555").add("openSpaceSensors.Temperature666"));
                    historian.rxGetTimeSeriesChunk(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responses : {}", rsp);
                                    long totalHit = rsp.getLong(RESPONSE_TOTAL_FOUND);
                                    assertEquals(2, totalHit);
                                    JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                                    assertEquals(2, docs.size());
                                    JsonObject doc1 = docs.getJsonObject(0);
                                    assertTrue(doc1.containsKey(RESPONSE_METRIC_NAME_FIELD));
                                    if (doc1.getString(RESPONSE_METRIC_NAME_FIELD).equals("openSpaceSensors.Temperature555")){
                                        checkTimeseriesChunks(doc1,docs.getJsonObject(1));
                                    } else {
                                        checkTimeseriesChunks(docs.getJsonObject(1), doc1);
                                    }
                                    testContext.completeNow();
                                });
                            })
                            .subscribe();
                })
                .subscribe();
    }
    private void checkTimeseriesChunks (JsonObject doc1, JsonObject doc2) {
        long time1 = 1477895624866L;
        long time2 = 1477916224866L;
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_START_FIELD));
        assertEquals(time1, doc1.getLong(RESPONSE_CHUNK_START_FIELD));
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_END_FIELD));
        assertEquals(time2, doc1.getLong(RESPONSE_CHUNK_END_FIELD));
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_ID_FIELD));
        /*assertEquals("092ec781-4901-4fe6-8a0c-ee278b8604aa",doc1.getString(RESPONSE_CHUNK_ID_FIELD));*/
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SIZE_FIELD));
        assertEquals(2,doc1.getLong(RESPONSE_CHUNK_SIZE_FIELD));
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_VALUE_FIELD));
        assertEquals("H4sIAAAAAAAAAOPi1GSAAAcuPoEDK1/C+AIOAgwAJ4b8wB0AAAA=", doc1.getString(RESPONSE_CHUNK_VALUE_FIELD));
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_WINDOW_MS_FIELD));
        assertEquals(20600000,doc1.getLong(RESPONSE_CHUNK_WINDOW_MS_FIELD));
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SIZE_BYTES_FIELD));
        assertEquals(38,doc1.getLong(RESPONSE_CHUNK_SIZE_BYTES_FIELD));
        assertTrue(doc1.containsKey(RESPONSE_CHUNK_VERSION_FIELD));
        /*assertEquals(1663577207679746048L,doc1.getLong(RESPONSE_CHUNK_VERSION_FIELD));*/
        assertEquals("openSpaceSensors.Temperature666",doc2.getString(RESPONSE_METRIC_NAME_FIELD));
        assertEquals(time1, doc2.getLong(RESPONSE_CHUNK_START_FIELD));
        assertEquals(time2, doc2.getLong(RESPONSE_CHUNK_END_FIELD));
        assertEquals(2,doc2.getLong(RESPONSE_CHUNK_SIZE_FIELD));
        assertEquals("H4sIAAAAAAAAAOPi1Dx7BgQ4HLj4BA6sfMmpOWsmCCg6CDAAAFASLIkdAAAA", doc2.getString(RESPONSE_CHUNK_VALUE_FIELD));
        assertEquals(20600000,doc2.getLong(RESPONSE_CHUNK_WINDOW_MS_FIELD));
        assertEquals(45,doc2.getLong(RESPONSE_CHUNK_SIZE_BYTES_FIELD));
    }

}

