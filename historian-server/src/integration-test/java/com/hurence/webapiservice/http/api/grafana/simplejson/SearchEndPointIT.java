package com.hurence.webapiservice.http.api.grafana.simplejson;


import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class SearchEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION_HISTORIAN;

    @BeforeAll
    public static void beforeAll(SolrClient client) throws IOException, SolrServerException {
        HistorianSolrITHelper
                .initHistorianSolr(client);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
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
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
    }


    @AfterEach
    public void afterEach(Vertx vertx) {
        vertx.deploymentIDs().forEach(vertx::undeploy);

    }

    @AfterAll
    public static void afterAll(Vertx vertx) {
        vertx.close();
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithQueryAndNoLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx)
                .doOnError(testContext::failNow)
                .subscribe(t -> {
                    assertRequestGiveObjectResponseFromFileWithNoOrder(vertx, testContext,
                            "/http/grafana/simplejson/search/test1/request.json",
                            "/http/grafana/simplejson/search/test1/expectedResponse.json");
                });
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithQueryAndLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx)
                .doOnError(testContext::failNow)
                .subscribe(t -> {
                    assertRequestGiveObjectResponseFromFileWithDeafaultSize(vertx, testContext,
                            "/http/grafana/simplejson/search/testWithLimit/request.json",
                            "/http/grafana/simplejson/search/testWithLimit/expectedResponse.json");
                });
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithEmptyQueryAndNoLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx)
                .doOnError(testContext::failNow)
                .subscribe(t -> {
                    assertRequestGiveObjectResponseFromFileWithDeafaultSize(vertx, testContext,
                            "/http/grafana/simplejson/search/testEmptyQuery/request.json",
                            "/http/grafana/simplejson/search/testEmptyQuery/expectedResponse.json");
                });
    }

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testWithEmptyQueryAndLimit(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx)
                .doOnError(testContext::failNow)
                .subscribe(t -> {
                    assertRequestGiveObjectResponseFromFileWithDeafaultSize(vertx, testContext,
                            "/http/grafana/simplejson/search/testEmptyQueryWithLimit/request.json",
                            "/http/grafana/simplejson/search/testEmptyQueryWithLimit/expectedResponse.json");
                });
    }


    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void testNoMatch(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        HttpWithHistorianSolrITHelper.deployHttpAndHistorianVerticle(container, vertx)
                .doOnError(testContext::failNow)
                .subscribe(t -> {
                    assertRequestGiveObjectResponseFromFileWithNoOrder(vertx, testContext,
                            "/http/grafana/simplejson/search/testNoMatch/request.json",
                            "/http/grafana/simplejson/search/testNoMatch/expectedResponse.json");
                });
    }

    public void assertRequestGiveObjectResponseFromFileWithNoOrder(Vertx vertx, VertxTestContext testContext,
                                                                   String requestFile, String responseFile) {
        WebClient webClient = HttpITHelper.buildWebClient(vertx);
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/search")
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer, testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray metrics = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedMetrics = new JsonArray(fileContent.getDelegate());
                        Set expectedSet = new HashSet();
                        Set set = new HashSet();
                        for (Object object : metrics) {
                            set.add(object);
                        }
                        for(Object object : expectedMetrics){
                            expectedSet.add(object);
                        }
                        assertEquals(expectedSet, set);
                        assertEquals(expectedMetrics.size(), metrics.size());
                        testContext.completeNow();
                    });
                }));
    }

    public void assertRequestGiveObjectResponseFromFileWithDeafaultSize(Vertx vertx, VertxTestContext testContext,
                                                                        String requestFile, String responseFile) {
        WebClient webClient = HttpITHelper.buildWebClient(vertx);
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/search")
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer, testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray metrics = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedMetrics = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedMetrics.size(), metrics.size());
                        testContext.completeNow();
                    });
                }));
    }

}
