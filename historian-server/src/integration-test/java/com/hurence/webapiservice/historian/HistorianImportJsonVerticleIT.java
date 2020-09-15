package com.hurence.webapiservice.historian;

import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.historian.modele.SchemaVersion;
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

import static com.hurence.webapiservice.historian.HistorianVerticle.CONFIG_SCHEMA_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianImportJsonVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianImportJsonVerticleIT.class);
    private static String COLLECTION = "historian";

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.createChunkCollection(client, container, SchemaVersion.getCurrentVersion());
        HistorianSolrITHelper
                .deployHistorianVerticle(container, vertx)
                .subscribe(id -> {
                            historian = HistorianService.createProxy(vertx.getDelegate(), "historian_service");
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
        JsonArray params = new JsonArray().add(new JsonObject()
                .put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature000")
                .put(HistorianServiceFields.POINTS,
                        new JsonArray()
                                .add(new JsonArray().add(time1).add(2.0))
                                .add(new JsonArray().add(time2).add(4.0))));
        JsonObject paramsObject = new JsonObject().put(HistorianServiceFields.POINTS, params).put(HistorianServiceFields.ORIGIN, "ingestion-json");
        historian.rxAddTimeSeries(paramsObject)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject expectedResponse = new JsonObject().put(HistorianServiceFields.TOTAL_ADDED_POINTS, 2).put(HistorianServiceFields.TOTAL_ADDED_CHUNKS, 1);
                        assertEquals(rsp, expectedResponse);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject("{\""+ HistorianServiceFields.FROM+"\":1477895614866," +
                            "\""+ HistorianServiceFields.TO+"\": 1477916925845," +
                            "\""+ HistorianServiceFields.NAMES+"\":[\"openSpaceSensors.Temperature000\"]," +
                            "\""+ HistorianServiceFields.TAGS+"\":{}," +
                            "\""+ HistorianServiceFields.SAMPLING_ALGO+"\":\"AVERAGE\"," +
                            "\""+ HistorianServiceFields.BUCKET_SIZE+"\":1," +
                            "\""+ HistorianServiceFields.MAX_POINT_BY_METRIC+"\":844" +
                            "}");
                    historian.rxGetTimeSeries(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responses : {}", rsp);
                                    long totalPoints = rsp.getLong(HistorianServiceFields.TOTAL_POINTS);
                                    assertEquals(2, totalPoints);
                                    JsonArray docs = rsp.getJsonArray(HistorianServiceFields.TIMESERIES);
                                    assertEquals(1, docs.size());
                                    JsonObject doc1 = docs.getJsonObject(0);
                                    assertEquals("openSpaceSensors.Temperature000", doc1.getString(HistorianServiceFields.NAME));
                                    JsonArray datapoints1 = doc1.getJsonArray(HistorianServiceFields.DATAPOINTS);
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
        JsonArray params = new JsonArray()
                .add(new JsonObject().put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature111")
                        .put(HistorianServiceFields.POINTS, new JsonArray()
                                .add(new JsonArray().add(time1).add(2.0))
                                .add(new JsonArray().add(time2).add(4.0))))
                .add(new JsonObject().put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature222")
                        .put(HistorianServiceFields.POINTS, new JsonArray()
                                .add(new JsonArray().add(time1).add(3.1))
                                .add(new JsonArray().add(time2).add(8.8))))
                .add(new JsonObject().put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature333")
                        .put(HistorianServiceFields.POINTS, new JsonArray()
                                .add(new JsonArray().add(time3).add(4.1))
                                .add(new JsonArray().add(time4).add(6.5))))
                .add(new JsonObject().put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature444")
                        .put(HistorianServiceFields.POINTS, new JsonArray()
                                .add(new JsonArray().add(time3).add(0.0))
                                .add(new JsonArray().add(time4).add(9.1))));
        JsonObject paramsObject = new JsonObject()
                .put(HistorianServiceFields.POINTS, params)
                .put(HistorianServiceFields.ORIGIN, "ingestion-json");
        historian.rxAddTimeSeries(paramsObject)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject expectedResponse = new JsonObject().put(HistorianServiceFields.TOTAL_ADDED_POINTS, 8).put(HistorianServiceFields.TOTAL_ADDED_CHUNKS, 4);
                        assertEquals(rsp, expectedResponse);
                    });
                })
                .doAfterSuccess(t -> {
                    JsonObject params1 = new JsonObject("{\""+ HistorianServiceFields.FROM+"\":1477895614866," +
                            "\""+ HistorianServiceFields.TO+"\": 1477916925845," +
                            "\""+ HistorianServiceFields.NAMES+"\":[\"openSpaceSensors.Temperature111\",\"openSpaceSensors.Temperature222\"," +
                            "\"openSpaceSensors.Temperature333\"," +
                            "\"openSpaceSensors.Temperature444\"]," +
                            "\""+ HistorianServiceFields.TAGS+"\":{}," +
                            "\""+ HistorianServiceFields.SAMPLING_ALGO+"\":\"AVERAGE\"," +
                            "\""+ HistorianServiceFields.BUCKET_SIZE+"\":1," +
                            "\""+ HistorianServiceFields.MAX_POINT_BY_METRIC+"\":844" +
                            "}");
                    historian.rxGetTimeSeries(params1)
                            .doOnError(testContext::failNow)
                            .doOnSuccess(rsp -> {
                                testContext.verify(() -> {
                                    LOGGER.info("responses : {}", rsp);
                                    long totalPoints = rsp.getLong(HistorianServiceFields.TOTAL_POINTS);
                                    assertEquals(8, totalPoints);
                                    JsonArray docs = rsp.getJsonArray(HistorianServiceFields.TIMESERIES);
                                    assertEquals(4, docs.size());
                                    JsonObject doc1 = docs.getJsonObject(0);
                                    assertEquals("openSpaceSensors.Temperature111", doc1.getString(HistorianServiceFields.NAME));
                                    JsonArray datapoints1 = doc1.getJsonArray(HistorianServiceFields.DATAPOINTS);
                                    assertEquals(new JsonArray("[[2.0,1477895624866],[4.0,1477916224866]]"), datapoints1);
                                    JsonObject doc2 = docs.getJsonObject(1);
                                    assertEquals("openSpaceSensors.Temperature222", doc2.getString(HistorianServiceFields.NAME));
                                    JsonArray datapoints2 = doc2.getJsonArray(HistorianServiceFields.DATAPOINTS);
                                    assertEquals(new JsonArray("[[3.1,1477895624866],[8.8,1477916224866]]"), datapoints2);
                                    JsonObject doc3 = docs.getJsonObject(2);
                                    assertEquals("openSpaceSensors.Temperature333", doc3.getString(HistorianServiceFields.NAME));
                                    JsonArray datapoints3 = doc3.getJsonArray(HistorianServiceFields.DATAPOINTS);
                                    assertEquals(new JsonArray("[[4.1,1477895724888],[6.5,1477916924845]]"), datapoints3);
                                    JsonObject doc4 = docs.getJsonObject(3);
                                    assertEquals("openSpaceSensors.Temperature444", doc4.getString(HistorianServiceFields.NAME));
                                    JsonArray datapoints4 = doc4.getJsonArray(HistorianServiceFields.DATAPOINTS);
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
        JsonArray params = new JsonArray()
                .add(new JsonObject().put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature555")
                        .put(HistorianServiceFields.POINTS, new JsonArray()
                                .add(new JsonArray().add(time1).add(2.0))
                                .add(new JsonArray().add(time2).add(4.0))))
        .add(new JsonObject().put(HistorianServiceFields.NAME, "openSpaceSensors.Temperature666")
                .put(HistorianServiceFields.POINTS, new JsonArray()
                        .add(new JsonArray().add(time1).add(3.1))
                        .add(new JsonArray().add(time2).add(8.8))));
        JsonObject paramsObject = new JsonObject()
                .put(HistorianServiceFields.POINTS, params)
                .put(HistorianServiceFields.ORIGIN, "ingestion-json");
        historian.rxAddTimeSeries(paramsObject)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonObject expectedResponse = new JsonObject().put(HistorianServiceFields.TOTAL_ADDED_POINTS, 4).put(HistorianServiceFields.TOTAL_ADDED_CHUNKS, 2);
                        assertEquals(rsp, expectedResponse);
                        testContext.completeNow();
                    });
                })
//                .doAfterSuccess(t -> {
//                    JsonObject params1 = new JsonObject()
//                            .put(HistorianServiceFields.NAMES,
//                                    new JsonArray()
//                                            .add("openSpaceSensors.Temperature555")
//                                            .add("openSpaceSensors.Temperature666"));
//                    historian.rxGetTimeSeriesChunk(params1)
//                            .doOnError(testContext::failNow)
//                            .doOnSuccess(rsp -> {
//                                testContext.verify(() -> {
//                                    LOGGER.info("responses : {}", rsp);
//                                    long totalHit = rsp.getLong(HistorianServiceFields.TOTAL);
//                                    assertEquals(2, totalHit);
//                                    JsonArray docs = rsp.getJsonArray(HistorianServiceFields.CHUNKS);
//                                    assertEquals(2, docs.size());
//                                    JsonObject doc1 = docs.getJsonObject(0);
//                                    assertTrue(doc1.containsKey(HistorianServiceFields.NAME));
//                                    if (doc1.getString(HistorianServiceFields.NAME).equals("openSpaceSensors.Temperature555")){
//                                        compareTimeseriesChunks(doc1,params.getJsonObject(1));
//                                    } else {
//                                        compareTimeseriesChunks(params.getJsonObject(1), doc1);
//                                    }
//                                    testContext.completeNow();
//                                });
//                            })
//                            .subscribe();
//                })
                .subscribe();
    }
}

