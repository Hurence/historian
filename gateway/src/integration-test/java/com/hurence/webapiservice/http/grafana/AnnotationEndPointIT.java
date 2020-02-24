package com.hurence.webapiservice.http.grafana;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
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
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.util.HistorianSolrITHelper.COLLECTION_ANNOTATION;

@ExtendWith({VertxExtension.class, SolrExtension.class})

public class AnnotationEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        LOGGER.info("Indexing some documents in {} collection", COLLECTION_ANNOTATION);
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("time", 1581648194070L);   // 2020-2-14T02:43:14.070Z
        doc.addField("text", "annotation 1");
        doc.addField("tags", new JsonArray().add("tag1").add("tag2"));
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("time", 1581651794070L);  // 2020-2-14T03:43:14.070Z
        doc1.addField("text", "annotation 2");
        doc1.addField("tags", new JsonArray().add("tag3").add("tag2"));
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("time", 1581655394070L);  // 2020-2-14T04:43:14.070Z
        doc2.addField("text", "annotation 3");
        doc2.addField("tags", new JsonArray().add("tag1").add("tag3"));
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField("time", 1581658994070L);  // 2020-2-14T05:43:14.070Z
        doc3.addField("text", "annotation 4");
        doc3.addField("tags", new JsonArray().add("tag4").add("tag2"));
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField("time", 1581662594070L);  // 2020-2-14T06:43:14.070Z
        doc4.addField("text", "annotattion 5");
        doc4.addField("tags", new JsonArray().add("tag3").add("tag4"));
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField("time", 1581666194070L);  // 2020-2-14T07:43:14.070Z
        doc5.addField("text", "annotation 6");
        doc5.addField("tags", new JsonArray().add("tag3").add("tag5"));
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField("time", 1581669794070L);  // 2020-2-14T08:43:14.070Z
        doc6.addField("text", "annotattion 7");
        doc.addField("tags", new JsonArray().add("tag2").add("tag3"));
        client.commit(COLLECTION_ANNOTATION);
        LOGGER.info("Indexed some documents in {} collection", COLLECTION_ANNOTATION);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/annotations");
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    /*@Timeout(value = 5, timeUnit = TimeUnit.SECONDS)*/
    public void testAnnotationWithTypeEqualsAll(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/annotation/extract-algo/test1/request.json",
                "/http/grafana/annotation/extract-algo/test1/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithMatchAnyEqualsTrue(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/annotation/extract-algo/test2/request.json",
                "/http/grafana/annotation/extract-algo/test2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithMatchAnyEqualsFalse(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/annotation/extract-algo/test3/request.json",
                "/http/grafana/annotation/extract-algo/test3/expectedResponse.json");
    }


    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveResponseFromFile(vertx, testContext, requestFile, responseFile);
    }
}
