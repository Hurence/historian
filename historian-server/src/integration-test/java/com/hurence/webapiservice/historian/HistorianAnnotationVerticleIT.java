package com.hurence.webapiservice.historian;

import com.hurence.historian.model.HistorianServiceFields;
import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.util.SolrITHelper;
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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
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

import static com.hurence.historian.model.HistorianAnnotationCollectionFields.ID;
import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianAnnotationVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION_ANNOTATION;

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        SolrITHelper.createChunkCollection(SolrITHelper.COLLECTION_HISTORIAN, SolrExtension.getSolr1Url(container), SchemaVersion.getCurrentVersion());
        HistorianSolrITHelper.createAnnotationCollection(client, container, SchemaVersion.getCurrentVersion());
        HistorianSolrITHelper
                .deployHistorianVerticle(container, vertx)
                .subscribe(id -> {
                            historian = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), "historian_service");
                            context.completeNow();
                        },
                        t -> context.failNow(t));
        LOGGER.info("Indexing some documents in {} collection", COLLECTION);

        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(ID, "doc");
        doc.addField(TIME, 1581648194070L);
        doc.addField(TEXT, "annotation 1");
        doc.addField(TAGS, new JsonArray().add("tag1").add("tag2"));
        client.add(COLLECTION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField(ID, "doc1");
        doc1.addField(TIME, 1581651794070L);
        doc1.addField(TEXT, "annotation 2");
        doc1.addField(TAGS, new JsonArray().add("tag3").add("tag2"));
        client.add(COLLECTION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField(ID, "doc2");
        doc2.addField(TIME, 1581655394070L);
        doc2.addField(TEXT, "annotation 3");
        doc2.addField(TAGS, new JsonArray().add("tag1").add("tag3"));
        client.add(COLLECTION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField(ID, "doc3");
        doc3.addField(TIME, 1581658994070L);
        doc3.addField(TEXT, "annotation 4");
        doc3.addField(TAGS, new JsonArray().add("tag4").add("tag2"));
        client.add(COLLECTION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField(ID, "doc4");
        doc4.addField(TIME, 1581662594070L);
        doc4.addField(TEXT, "annotattion 5");
        doc4.addField(TAGS, new JsonArray().add("tag3").add("tag4"));
        client.add(COLLECTION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField(ID, "doc5");
        doc5.addField(TIME, 1581666194070L);
        doc5.addField(TEXT, "annotation 6");
        doc5.addField(TAGS, new JsonArray().add("tag3").add("tag5"));
        client.add(COLLECTION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField(ID, "doc6");
        doc6.addField(TIME, 1581669794070L);
        doc6.addField(TEXT, "annotattion 7");
        doc6.addField(TAGS, new JsonArray().add("tag2").add("tag3"));
        client.add(COLLECTION, doc6);
        client.commit(COLLECTION);

        LOGGER.info("Indexed some documents in {} collection", COLLECTION);
    }

    @AfterAll
    static void finish(SolrClient client, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        LOGGER.debug("deleting collection {}", COLLECTION);
        final CollectionAdminRequest.Delete deleteRequest = CollectionAdminRequest.deleteCollection(COLLECTION);
        client.request(deleteRequest);
        LOGGER.debug("closing vertx");
        vertx.close(context.completing());
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithTypeEqualsAll (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581651394000L)
                .put(TO, 1581666194000L)
                .put(LIMIT, 100)
                .put(FIELD_TAGS, new JsonArray().add("tag1").add("tag2"))
                .put(MATCH_ANY, true)
                .put(TYPE, AnnotationRequestType.ALL.toString());
        LOGGER.debug("params json is : {} ", params);
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        int totalHit = rsp.getInteger(HistorianServiceFields.TOTAL_HIT);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (4, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }
    @Test
    @Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void testAnnotationWithMatchAnyEqualsTrue (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581644594000L)
                .put(TO, 1581663014000L)
                .put(LIMIT, 100)
                .put(FIELD_TAGS, new JsonArray().add("tag1").add("tag2"))
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
    void testAnnotationWithMatchAnyEqualsFalse (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581644594000L)
                .put(TO, 1581663014000L)
                .put(LIMIT, 100)
                .put(FIELD_TAGS, new JsonArray().add("tag1").add("tag2"))
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
    void testAnnotationWithLimit (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put (FROM, 1581644594000L)
                .put(TO, 1581663014000L)
                .put(LIMIT, 2)
                .put(FIELD_TAGS, new JsonArray().add("tag1").add("tag2"))
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
    void testAnnotationWithNoTime (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put(LIMIT, 10)
                .put(FIELD_TAGS, new JsonArray().add("tag1").add("tag2"))
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
    void testAnnotationWithEmptyQuery (VertxTestContext testContext) {
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

