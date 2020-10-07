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
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hurence.historian.modele.HistorianServiceFields.METRICS;
import static com.hurence.historian.modele.HistorianServiceFields.TOTAL;
import static com.hurence.timeseries.model.Definitions.SOLR_COLUMN_ID;
import static com.hurence.timeseries.model.Definitions.SOLR_COLUMN_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianSearchVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION =  HistorianSolrITHelper.COLLECTION_HISTORIAN;

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.createChunkCollection(client, container, SchemaVersion.getCurrentVersion());
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);

        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc.addField(SOLR_COLUMN_NAME, "Amazon Kindle Paperwhite");
        client.add(COLLECTION, doc);
        final SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc1.addField(SOLR_COLUMN_NAME, "upper_50");
        client.add(COLLECTION, doc1);
        final SolrInputDocument doc2 = new SolrInputDocument();
        doc2.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc2.addField(SOLR_COLUMN_NAME, "Amazon");
        client.add(COLLECTION, doc2);
        final SolrInputDocument doc3 = new SolrInputDocument();
        doc3.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc3.addField(SOLR_COLUMN_NAME, "Amazon Kindle Paperblack");
        client.add(COLLECTION, doc3);
        final SolrInputDocument doc4 = new SolrInputDocument();
        doc4.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc4.addField(SOLR_COLUMN_NAME, "upper_75");
        client.add(COLLECTION, doc4);
        final SolrInputDocument doc5 = new SolrInputDocument();
        doc5.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc5.addField(SOLR_COLUMN_NAME, "upper_90");
        client.add(COLLECTION, doc5);
        final SolrInputDocument doc6 = new SolrInputDocument();
        doc6.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc6.addField(SOLR_COLUMN_NAME, "up");
        client.add(COLLECTION, doc6);
        final SolrInputDocument doc7 = new SolrInputDocument();
        doc7.addField(SOLR_COLUMN_ID, UUID.randomUUID().toString());
        doc7.addField(SOLR_COLUMN_NAME, "upper_50");
        client.add(COLLECTION, doc7);
        client.commit(COLLECTION, true, true);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        HistorianSolrITHelper
                .deployHistorianVerticle(container, vertx)
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
    @ Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void getMetricsNameTest (VertxTestContext testContext) {
        JsonObject params = new JsonObject ()
                .put(HistorianServiceFields.METRIC, "per");
        historian.rxGetMetricsName (params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        LOGGER.info("docs {} ",rsp);
                        assertEquals (5, rsp.getLong(TOTAL));
                        JsonArray docs = rsp.getJsonArray (METRICS);
                        LOGGER.info("docs {}",docs);
                        assertEquals (5, docs.size ());
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }

    @Test
    @ Timeout (value = 5, timeUnit = TimeUnit.SECONDS)
    void EmptyTest (VertxTestContext testContext) {
        JsonObject params = new JsonObject ();
        historian.rxGetMetricsName(params)
                .doOnError (testContext :: failNow)
                .doOnSuccess (rsp -> {
                    testContext.verify (() -> {
                        LOGGER.info("docs {} ",rsp);
                        assertEquals (7, rsp.getLong(TOTAL));
                        JsonArray docs = rsp.getJsonArray (METRICS);
                        LOGGER.info("docs {}",docs);
                        assertEquals (7, docs.size ());
                        testContext.completeNow ();
                    });
                })
                .subscribe ();
    }

}

