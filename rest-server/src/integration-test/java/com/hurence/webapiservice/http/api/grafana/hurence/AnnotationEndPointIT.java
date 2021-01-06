package com.hurence.webapiservice.http.api.grafana.hurence;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
import java.util.concurrent.TimeUnit;

import static com.hurence.historian.model.HistorianServiceFields.ANNOTATIONS;
import static com.hurence.webapiservice.historian.HistorianVerticle.CONFIG_SCHEMA_VERSION;
import static com.hurence.webapiservice.util.HistorianSolrITHelper.COLLECTION_ANNOTATION;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})

public class AnnotationEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(AnnotationEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0);
        SolrITHelper.createAnnotationCollection(client, SolrExtension.getSolr1Url(container), SchemaVersion.VERSION_0);
        LOGGER.info("Indexing some documents in {} collection", COLLECTION_ANNOTATION);
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        doc.addField("time", 1581648194070L);   // 2020-2-14T02:43:14.070Z
        doc.addField("text", "annotation 1");
        doc.addField("tags", new JsonArray().add("tag1").add("tag2"));
        final UpdateResponse updateResponse = client.add(COLLECTION_ANNOTATION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("id", "2");
        doc1.addField("time", 1581651794070L);  // 2020-2-14T03:43:14.070Z
        doc1.addField("text", "annotation 2");
        doc1.addField("tags", new JsonArray().add("tag3").add("tag2"));
        final UpdateResponse updateResponse1 = client.add(COLLECTION_ANNOTATION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("id", "3");
        doc2.addField("time", 1581655394070L);  // 2020-2-14T04:43:14.070Z
        doc2.addField("text", "annotation 3");
        doc2.addField("tags", new JsonArray().add("tag1").add("tag3"));
        final UpdateResponse updateResponse2 = client.add(COLLECTION_ANNOTATION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField("id", "4");
        doc3.addField("time", 1581658994070L);  // 2020-2-14T05:43:14.070Z
        doc3.addField("text", "annotation 4");
        doc3.addField("tags", new JsonArray().add("tag4").add("tag2"));
        final UpdateResponse updateResponse3 = client.add(COLLECTION_ANNOTATION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField("id", "doc4");
        doc4.addField("time", 1581662594070L);  // 2020-2-14T06:43:14.070Z
        doc4.addField("text", "annotation 5");
        doc4.addField("tags", new JsonArray().add("tag3").add("tag4"));
        final UpdateResponse updateResponse4 = client.add(COLLECTION_ANNOTATION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField("id", "doc5");
        doc5.addField("time", 1581666194070L);  // 2020-2-14T07:43:14.070Z
        doc5.addField("text", "annotation 6");
        doc5.addField("tags", new JsonArray().add("tag3").add("tag5"));
        final UpdateResponse updateResponse5 = client.add(COLLECTION_ANNOTATION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField("id", "doc6");
        doc6.addField("time", 1581669794070L);  // 2020-2-14T08:43:14.070Z
        doc6.addField("text", "annotation 7");
        doc6.addField("tags", new JsonArray().add("tag2").add("tag3"));
        final UpdateResponse updateResponse6 = client.add(COLLECTION_ANNOTATION, doc6);
        client.commit(COLLECTION_ANNOTATION);
        LOGGER.info("Indexed some documents in {} collection", COLLECTION_ANNOTATION);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_ANNOTATIONS_API_ENDPOINT);
        JsonObject historianConf = new JsonObject()
                .put(CONFIG_SCHEMA_VERSION,
                        SchemaVersion.VERSION_0.toString());
        HttpWithHistorianSolrITHelper.deployHttpAndCustomHistorianVerticle(container, vertx, historianConf).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithTypeEqualsAll(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/annotation/testWithTypeEqualsAll/request.json",
                "/http/grafana/hurence/annotation/testWithTypeEqualsAll/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithMatchAnyEqualsTrue(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFileWithNoOrder(vertx, testContext,
                "/http/grafana/hurence/annotation/testMatchAnyEqualsTrue/request.json",
                "/http/grafana/hurence/annotation/testMatchAnyEqualsTrue/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithMatchAnyEqualsFalse(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/annotation/testMatchAnyEqualsFalse/request.json",
                "/http/grafana/hurence/annotation/testMatchAnyEqualsFalse/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithLimit(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/annotation/testLimitNumberOfTags/request.json",
                "/http/grafana/hurence/annotation/testLimitNumberOfTags/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithNoTime(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFileWithNoOrder(vertx, testContext,
                "/http/grafana/hurence/annotation/testRequestWithNoTime/request.json",
                "/http/grafana/hurence/annotation/testRequestWithNoTime/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAnnotationWithEmptyQuery(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveObjectResponseFromFile(vertx, testContext,
                "/http/grafana/hurence/annotation/testEmptyQuery/request.json",
                "/http/grafana/hurence/annotation/testEmptyQuery/expectedResponse.json");
    }


    public void assertRequestGiveObjectResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveObjectResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

    public void assertRequestGiveObjectResponseFromFileWithNoOrder(Vertx vertx, VertxTestContext testContext,
                                                                   String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(HttpServerVerticle.HURENCE_DATASOURCE_GRAFANA_ANNOTATIONS_API_ENDPOINT)
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer, testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        JsonArray annotationExpected = expectedBody.getJsonArray(ANNOTATIONS);
                        JsonArray annotations = body.getJsonArray(ANNOTATIONS);
                        Set expectedSet = new HashSet();
                        Set set = new HashSet();
                        for (Object object : annotations) {
                            set.add(object);
                        }
                        for(Object object : annotationExpected){
                            expectedSet.add(object);
                        }
                        assertEquals(expectedSet, set);
                        testContext.completeNow();
                    });
                }));
    }
}
