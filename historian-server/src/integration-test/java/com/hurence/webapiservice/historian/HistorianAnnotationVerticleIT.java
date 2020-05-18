package com.hurence.webapiservice.historian;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.http.api.grafana.modele.AnnotationRequestType;
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

import static com.hurence.historian.modele.HistorianFields.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianAnnotationVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION_ANNOTATION;

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.createAnnotationCollection(client, container, SchemaVersion.VERSION_0);
        HistorianSolrITHelper
                .deployHistorienVerticle(container, vertx)
                .subscribe(id -> {
                            historian = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), "historian_service");
                            context.completeNow();
                        },
                        t -> context.failNow(t));
        LOGGER.info("Indexing some documents in {} collection", COLLECTION);

        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("time", 1581648194070L);
        doc.addField("text", "annotation 1");
        doc.addField("tags", new JsonArray().add("tag1").add("tag2"));
        final UpdateResponse updateResponse = client.add(COLLECTION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("time", 1581651794070L);
        doc1.addField("text", "annotation 2");
        doc1.addField("tags", new JsonArray().add("tag3").add("tag2"));
        final UpdateResponse updateResponse1 = client.add(COLLECTION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("time", 1581655394070L);
        doc2.addField("text", "annotation 3");
        doc2.addField("tags", new JsonArray().add("tag1").add("tag3"));
        final UpdateResponse updateResponse2 = client.add(COLLECTION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField("time", 1581658994070L);
        doc3.addField("text", "annotation 4");
        doc3.addField("tags", new JsonArray().add("tag4").add("tag2"));
        final UpdateResponse updateResponse3 = client.add(COLLECTION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField("time", 1581662594070L);
        doc4.addField("text", "annotattion 5");
        doc4.addField("tags", new JsonArray().add("tag3").add("tag4"));
        final UpdateResponse updateResponse4 = client.add(COLLECTION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField("time", 1581666194070L);
        doc5.addField("text", "annotation 6");
        doc5.addField("tags", new JsonArray().add("tag3").add("tag5"));
        final UpdateResponse updateResponse5 = client.add(COLLECTION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField("time", 1581669794070L);
        doc6.addField("text", "annotattion 7");
        doc6.addField("tags", new JsonArray().add("tag2").add("tag3"));
        final UpdateResponse updateResponse6 = client.add(COLLECTION, doc6);
        client.commit(COLLECTION);

        LOGGER.info("Indexed some documents in {} collection", COLLECTION);
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
    void testAnnotationWithTypeEqualsAll (VertxTestContext testContext) throws InterruptedException {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581651394000L)
                .put(TO, 1581666194000L)
                .put(LIMIT, 100)
                .put(TAGS, new JsonArray().add("tag1").add("tag2"))
                .put(MATCH_ANY, true)
                .put(TYPE, AnnotationRequestType.ALL.toString());
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (4, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }
    @Test
    @Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithMatchAnyEqualsTrue (VertxTestContext testContext) throws InterruptedException {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581644594000L)
                .put(TO, 1581663014000L)
                .put(LIMIT, 100)
                .put(TAGS, new JsonArray().add("tag1").add("tag2"))
                .put(MATCH_ANY, true)
                .put(TYPE, AnnotationRequestType.TAGS.toString());
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (4, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }


    @Test
    @Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithMatchAnyEqualsFalse (VertxTestContext testContext) throws InterruptedException {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581644594000L)
                .put(TO, 1581663014000L)
                .put(LIMIT, 100)
                .put(TAGS, new JsonArray().add("tag1").add("tag2"))
                .put(MATCH_ANY, false)
                .put(TYPE, AnnotationRequestType.TAGS.toString());
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (1, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }
    @Test
    @Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithLimit (VertxTestContext testContext) throws InterruptedException {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581644594000L)
                .put(TO, 1581663014000L)
                .put(LIMIT, 2)
                .put(TAGS, new JsonArray().add("tag1").add("tag2"))
                .put(MATCH_ANY, true)
                .put(TYPE, AnnotationRequestType.TAGS.toString());
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (2, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }

    @Test
    @Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithNoTime (VertxTestContext testContext) throws InterruptedException {
        JsonObject params = new JsonObject ()
                .put(LIMIT, 10)
                .put(TAGS, new JsonArray().add("tag1").add("tag2"))
                .put(MATCH_ANY, true)
                .put(TYPE, AnnotationRequestType.TAGS.toString());
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (5, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }

    @Test
    @Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithEmptyQuery (VertxTestContext testContext) throws InterruptedException {
        JsonObject params = new JsonObject ("{}");
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (7, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }


}

