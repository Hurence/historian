package com.hurence.webapiservice.http.grafana;


import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static com.hurence.webapiservice.historian.HistorianFields.RESPONSE_TOTAL_METRICS_RETURNED;
import static com.hurence.webapiservice.historian.HistorianVerticle.CONFIG_API_HISTORAIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class SearchEndPointWithCustomConfigIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointWithCustomConfigIT.class);
    private static WebClient webClient;
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION;

    @BeforeAll
    public static void beforeAll(SolrClient client, Vertx vertx) throws IOException, SolrServerException {
        HistorianSolrITHelper
                .initHistorianSolr(client);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", UUID.randomUUID().toString());
        doc.addField("name", "Amazon Kindle Paperwhite");
        client.add(COLLECTION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("id", UUID.randomUUID().toString());
        doc1.addField("name", "upper_50");
        client.add(COLLECTION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("id", UUID.randomUUID().toString());
        doc2.addField("name", "Amazon");
        client.add(COLLECTION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField("id", UUID.randomUUID().toString());
        doc3.addField("name", "Amazon Kindle Paperblack");
        client.add(COLLECTION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField("id", UUID.randomUUID().toString());
        doc4.addField("name", "upper_75");
        client.add(COLLECTION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField("id", UUID.randomUUID().toString());
        doc5.addField("name", "upper_90");
        client.add(COLLECTION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField("id", UUID.randomUUID().toString());
        doc6.addField("name", "up");
        client.add(COLLECTION, doc6);
        final SolrInputDocument doc7 = new SolrInputDocument();
        doc7.addField("id", UUID.randomUUID().toString());
        doc7.addField("name", "upper_50");
        client.add(COLLECTION, doc7);
        client.commit(COLLECTION);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        webClient = HttpITHelper.buildWebClient(vertx);
    }


    @AfterEach
    public void afterEach(Vertx vertx) {
        vertx.deploymentIDs().forEach(vertx::undeploy);
    }

    @AfterAll
    public static void afterAll(Vertx vertx) {
        webClient.close();
        vertx.close();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithQueryAndNoLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject grafana = new JsonObject().put(HistorianVerticle.CONFIG_GRAFANA_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_SEARCH_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_DEFAULT_SIZE_HISTORAIN, 100)));
        JsonObject apiDefault = new JsonObject().put(CONFIG_API_HISTORAIN, grafana);
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx, apiDefault)
                .map(t -> {
                    assertRequestGiveObjectResponseFromFileWithNoOrder(vertx, testContext,
                            "/http/grafana/search/test1/request.json",
                            "/http/grafana/search/test1/expectedResponse.json");
                    return t;
                }).subscribe();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithQueryAndLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject grafana = new JsonObject().put(HistorianVerticle.CONFIG_GRAFANA_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_SEARCH_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_DEFAULT_SIZE_HISTORAIN, 2)));
        JsonObject apiDefault = new JsonObject().put(CONFIG_API_HISTORAIN, grafana);
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx, apiDefault)
                .map(t -> {
                    assertRequestGiveObjectResponseFromFileWithDeafaultSize(vertx, testContext,
                            "/http/grafana/search/testWithLimit/request.json",
                            "/http/grafana/search/testWithLimit/expectedResponse.json");
                    return t;
                }).subscribe();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithEmptyQueryAndNoLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject grafana = new JsonObject().put(HistorianVerticle.CONFIG_GRAFANA_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_SEARCH_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_DEFAULT_SIZE_HISTORAIN, 100)));
        JsonObject apiDefault = new JsonObject().put(CONFIG_API_HISTORAIN, grafana);
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx, apiDefault)
                .map(t -> {
                    assertRequestGiveObjectResponseFromFileWithDeafaultSize(vertx, testContext,
                            "/http/grafana/search/testEmptyQuery/request.json",
                            "/http/grafana/search/testEmptyQuery/expectedResponse.json");
                    return t;
                }).subscribe();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithEmptyQueryAndLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject grafana = new JsonObject().put(HistorianVerticle.CONFIG_GRAFANA_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_SEARCH_HISTORAIN, new JsonObject().put(HistorianVerticle.CONFIG_DEFAULT_SIZE_HISTORAIN, 2)));
        JsonObject apiDefault = new JsonObject().put(CONFIG_API_HISTORAIN, grafana);
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx, apiDefault)
                .map(t -> {
                    assertRequestGiveObjectResponseFromFileWithDeafaultSize(vertx, testContext,
                            "/http/grafana/search/testEmptyQueryWithLimit/request.json",
                            "/http/grafana/search/testEmptyQueryWithLimit/expectedResponse.json");
                    return t;
                }).subscribe();
    }

    public void assertRequestGiveObjectResponseFromFileWithNoOrder(Vertx vertx, VertxTestContext testContext,
                                                                   String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/search")
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        JsonArray metrics = body.getJsonArray(RESPONSE_METRICS);
                        JsonArray expectedMetrics = expectedBody.getJsonArray(RESPONSE_METRICS);
                        Set expectedSet = new HashSet();
                        Set set = new HashSet();
                        for (Object object : metrics) {
                            set.add(object);
                        }
                        for(Object object : expectedMetrics){
                            expectedSet.add(object);
                        }
                        assertEquals(expectedSet, set);
                        assertEquals(expectedBody.getInteger(RESPONSE_TOTAL_METRICS_RETURNED), body.getInteger(RESPONSE_TOTAL_METRICS_RETURNED));
                        testContext.completeNow();
                    });
                }));
    }

    public void assertRequestGiveObjectResponseFromFileWithDeafaultSize(Vertx vertx, VertxTestContext testContext,
                                                                        String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/search")
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        JsonArray metrics = body.getJsonArray(RESPONSE_METRICS);
                        JsonArray expectedMetrics = expectedBody.getJsonArray(RESPONSE_METRICS);
                        assertEquals(expectedMetrics.size(), metrics.size());
                        assertEquals(expectedBody.getInteger(RESPONSE_TOTAL_METRICS_RETURNED), body.getInteger(RESPONSE_TOTAL_METRICS_RETURNED));
                        testContext.completeNow();
                    });
                }));
    }

}
