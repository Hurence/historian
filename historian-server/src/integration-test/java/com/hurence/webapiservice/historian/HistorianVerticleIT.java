package com.hurence.webapiservice.historian;

import com.hurence.historian.model.HistorianServiceFields;
import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
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
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.hurence.historian.model.HistorianChunkCollectionFieldsVersionCurrent.ID;
import static com.hurence.historian.model.HistorianServiceFields.*;
import static com.hurence.timeseries.model.Definitions.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = "historian";

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.createChunkCollection(client, container, SchemaVersion.getCurrentVersion());
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        GeneralInjectorCurrentVersion injector = new GeneralInjectorCurrentVersion();
        Chunk chunk1 = ChunkBuilderHelper.fromPoints("temp_a",
                Arrays.asList(
                        Measure.fromValue( 1L, 5),
                        Measure.fromValue( 2L, 8),
                        Measure.fromValue( 3L, 1.2),
                        Measure.fromValue( 4L, 6.5)
                ));
        injector.addChunk(chunk1);
        Chunk chunk2 = ChunkBuilderHelper.fromPoints("temp_a",
                Arrays.asList(
                        Measure.fromValue( 5L, -2),
                        Measure.fromValue( 6L, 8.8),
                        Measure.fromValue( 7L, 13.3),
                        Measure.fromValue( 8L, 2)
                ));
        injector.addChunk(chunk2);
        Chunk chunk3 = ChunkBuilderHelper.fromPoints("temp_a",
                Arrays.asList(
                        Measure.fromValue( 9L, -5),
                        Measure.fromValue( 10L, 80),
                        Measure.fromValue( 11L, 1.2),
                        Measure.fromValue( 12L, 5.5)
                ));
        injector.addChunk(chunk3);
        Chunk chunk4 = ChunkBuilderHelper.fromPoints("temp_b",
                Arrays.asList(
                        Measure.fromValue( 9L, -5),
                        Measure.fromValue( 10L, 80),
                        Measure.fromValue( 11L, 1.2),
                        Measure.fromValue( 12L, 5.5)
                ));
        injector.addChunk(chunk4);
        injector.injectChunks(client);
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
    public void testSchemaRequest(SolrClient client) throws Exception {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION);
        assertValidSchemaResponse(schemaResponse);
        SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();
        assertNotNull(schemaRepresentation);
        assertEquals("default-config", schemaRepresentation.getName());
        assertEquals(1.6, schemaRepresentation.getVersion(), 0.001f);
        assertEquals(ID, schemaRepresentation.getUniqueKey());
        assertEquals(32, schemaRepresentation.getFields().size());
        assertEquals(69, schemaRepresentation.getDynamicFields().size());
        assertEquals(69, schemaRepresentation.getFieldTypes().size());
        assertEquals(0, schemaRepresentation.getCopyFields().size());
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithoutParameter(VertxTestContext testContext) {
        JsonObject params = new JsonObject();
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(HistorianServiceFields.TOTAL);
                        assertEquals(4, totalHit);
                        JsonArray docs = rsp.getJsonArray(CHUNKS);
                        assertEquals(4, docs.size());
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertTrue(doc1.containsKey(SOLR_COLUMN_NAME));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_START));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_END));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_AVG));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_ID));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_COUNT));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_SAX));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_VALUE));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_MIN));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_MAX));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_TREND));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_SUM));
                        assertTrue(doc1.containsKey(SOLR_COLUMN_FIRST));
                        assertEquals(28, doc1.size());
                        assertEquals("c598923bb1aa77c6bec68bf64146633339fe22a7c85dcf0a9a49386ff38b4d8e", doc1.getString(ID));
                        assertEquals(1L, doc1.getLong(SOLR_COLUMN_START));
                        assertEquals(4L, doc1.getLong(SOLR_COLUMN_END));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("a17c15b77fa8b7c2f31099d9d2168ca339a031f84c2ef024a2ca26c02eedf9a3", doc2.getString(ID));
                        assertEquals(5L, doc2.getLong(SOLR_COLUMN_START));
                        assertEquals(8L, doc2.getLong(SOLR_COLUMN_END));
                        JsonObject doc3 = docs.getJsonObject(2);
                        assertEquals("05e93d0232a6b8ff65de193c251a3369c2d179ff2df5a3f5d85ef304c71e9a47", doc3.getString(ID));
                        assertEquals(9L, doc3.getLong(SOLR_COLUMN_START));
                        assertEquals(12L, doc3.getLong(SOLR_COLUMN_END));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithStart(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(FROM, 9L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(CHUNKS);
                        JsonObject doc2 = docs.getJsonObject(0);
                        assertEquals("05e93d0232a6b8ff65de193c251a3369c2d179ff2df5a3f5d85ef304c71e9a47", doc2.getString(ID));
                        JsonObject doc3 = docs.getJsonObject(1);
                        assertEquals("76b6954755cee70cea39e10ea28629657ba62f0fd6cd84f72d30bf26510ea16d", doc3.getString(ID));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithEnd(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(TO, 1571129390801L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals("c598923bb1aa77c6bec68bf64146633339fe22a7c85dcf0a9a49386ff38b4d8e", doc1.getString(ID));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("a17c15b77fa8b7c2f31099d9d2168ca339a031f84c2ef024a2ca26c02eedf9a3", doc2.getString(ID));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    @Disabled("This feature is legacy, now this is the service that decides what to return based on timeseries request.")
    void getTimeSeriesChunkTestWithSelectedFields(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(HistorianServiceFields.FIELDS, new JsonArray()
                        .add(SOLR_COLUMN_VALUE)
                        .add(SOLR_COLUMN_START)
                        .add(SOLR_COLUMN_MAX)
                        .add(ID)
                );
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals(4, doc1.size());
                        assertEquals("id0", doc1.getString(ID));
                        assertEquals(1L, doc1.getLong(SOLR_COLUMN_START));
                        assertEquals(8.0, doc1.getDouble(SOLR_COLUMN_MAX));
                        assertEquals("H4sIAAAAAAAAAOPi1GQAAxEHLm4FRihHwYGLU9MYDD7bc3ELwMSlHAQYANb3vjkyAAAA",
                                doc1.getString(SOLR_COLUMN_VALUE));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithName(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(NAMES, Arrays.asList("temp_a"));
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(TOTAL);
                        assertEquals(3, totalHit);
                        JsonArray docs = rsp.getJsonArray(CHUNKS);
                        assertEquals(3, docs.size());
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    private static void assertValidSchemaResponse(SolrResponseBase schemaResponse) {
        assertEquals(0, schemaResponse.getStatus(), "Response contained errors: " + schemaResponse.toString());
        assertNull(schemaResponse.getResponse().get(ERRORS), "Response contained errors: " + schemaResponse.toString());
    }

}
