package com.hurence.webapiservice.http.grafana;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION_HISTORIAN;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", UUID.randomUUID().toString());
        doc.addField("name", "Amazon Kindle Paperwhite");
        final UpdateResponse updateResponse = client.add(COLLECTION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("id", UUID.randomUUID().toString());
        doc1.addField("name", "upper_50");
        final UpdateResponse updateResponse1 = client.add(COLLECTION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("id", UUID.randomUUID().toString());
        doc2.addField("name", "Amazon");
        final UpdateResponse updateResponse2 = client.add(COLLECTION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField("id", UUID.randomUUID().toString());
        doc3.addField("name", "Amazon Kindle Paperblack");
        final UpdateResponse updateResponse3 = client.add(COLLECTION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField("id", UUID.randomUUID().toString());
        doc4.addField("name", "upper_75");
        final UpdateResponse updateResponse4 = client.add(COLLECTION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField("id", UUID.randomUUID().toString());
        doc5.addField("name", "upper_90");
        final UpdateResponse updateResponse5 = client.add(COLLECTION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField("id", UUID.randomUUID().toString());
        doc6.addField("name", "up");
        final UpdateResponse updateResponse6 = client.add(COLLECTION, doc6);
        client.commit(COLLECTION);

        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/export/csv");
    }


    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    /*@Timeout(value = 5, timeUnit = TimeUnit.SECONDS)*/
    public void testSearch(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFileWithNoOrder(vertx, testContext,
                "/http/grafana/search/test1/request.json",
                "/http/grafana/search/test1/expectedResponse.json");
    }

    public void assertRequestGiveObjectResponseFromFileWithNoOrder(Vertx vertx, VertxTestContext testContext,
                                                                   String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/export/csv")
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        Set expectedSet = new HashSet();
                        Set set = new HashSet();
                        for (Object object : body) {
                            set.add(object);
                        }
                        for(Object object : expectedBody){
                            expectedSet.add(object);
                        }
                        assertEquals(expectedSet, set);
                        testContext.completeNow();
                    });
                }));
    }
}
