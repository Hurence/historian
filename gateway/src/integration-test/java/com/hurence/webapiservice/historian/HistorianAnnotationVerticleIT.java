package com.hurence.webapiservice.historian;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianAnnotationVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION_ANNOTATTION;

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
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_ANNOTATTION);

        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("time", 1581648194);
        doc.addField("text", "annotation 1");
        doc.addField("tags", new JsonArray().add("tag1").add("tag2"));
        final UpdateResponse updateResponse = client.add(COLLECTION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("time", 1581651794);
        doc1.addField("text", "annotation 2");
        doc1.addField("tags", new JsonArray().add("tag3").add("tag2"));
        final UpdateResponse updateResponse1 = client.add(COLLECTION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField("time", 1581655394);
        doc2.addField("text", "annotation 3");
        doc2.addField("tags", new JsonArray().add("tag1").add("tag3"));
        final UpdateResponse updateResponse2 = client.add(COLLECTION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField("time", 1581658994);
        doc3.addField("text", "annotation 4");
        doc3.addField("tags", new JsonArray().add("tag4").add("tag2"));
        final UpdateResponse updateResponse3 = client.add(COLLECTION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField("time", 1581662594);
        doc4.addField("text", "annotattion 5");
        doc4.addField("tags", new JsonArray().add("tag3").add("tag4"));
        final UpdateResponse updateResponse4 = client.add(COLLECTION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField("time", 1581666194);
        doc5.addField("text", "annotation 6");
        doc5.addField("tags", new JsonArray().add("tag3").add("tag5"));
        final UpdateResponse updateResponse5 = client.add(COLLECTION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField("time", 1581669794);
        doc6.addField("text", "annotattion 7");
        doc.addField("tags", new JsonArray().add("tag2").add("tag3"));
        final UpdateResponse updateResponse6 = client.add(COLLECTION, doc6);
        client.commit(COLLECTION);

        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_ANNOTATTION);
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
    /*@ Timeout (value = 5, timeUnit = TimeUnit.SECONDS)*/
    void getAnnotattionsTest (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put (FROM_REQUEST_FIELD, "2020-2-14T04:43:14.070Z")
                .put(TO_REQUEST_FIELD, "2020-2-14T07:43:14.070Z")
                .put(MAX_ANNOTATION_REQUEST_FIELD, 10)
                .put(TAGS_REQUEST_FIELD, new JsonArray().add("tag1"))
                .put(MATCH_ANY_REQUEST_FIELD, false)
                .put(TYPE_REQUEST_FIELD, "tags");
        historian.rxGetAnnotations (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        long totalHit = rsp.getLong (RESPONSE_TOTAL_FOUND);
                        LOGGER.info("annotations {} ",rsp);
                        assertEquals (1, totalHit);
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }

}
