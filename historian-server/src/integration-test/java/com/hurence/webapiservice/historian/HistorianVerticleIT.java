package com.hurence.webapiservice.historian;

import com.hurence.historian.modele.HistorianChunkCollectionFieldsVersion0;
import com.hurence.historian.modele.HistorianServiceFields;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.historian.solr.injector.Version0SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags;
import com.hurence.timeseries.model.Point;
import com.hurence.historian.solr.injector.GeneralInjectorCurrentVersion;
import com.hurence.historian.solr.util.ChunkBuilderHelper;
import com.hurence.timeseries.modele.chunk.ChunkVersionCurrent;
import com.hurence.timeseries.modele.points.PointImpl;
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
        ChunkVersionCurrent chunk1 = ChunkBuilderHelper.fromPoints("temp_a",
                Arrays.asList(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                ),
                Arrays.asList(
                        Arrays.asList(
                                new Point( 1L, 5),
                                new Point( 2L, 8),
                                new Point( 3L, 1.2),
                                new Point( 4L, 6.5)
                        ),
                        Arrays.asList(
                                new Point( 5L, -2),
                                new Point( 6L, 8.8),
                                new Point( 7L, 13.3),
                                new Point( 8L, 2)
                        ),
                        Arrays.asList(
                                new Point( 9L, -5),
                                new Point( 10L, 80),
                                new Point( 11L, 1.2),
                                new Point( 12L, 5.5)
                        )
                        new PointImpl( 1L, 5),
                        new PointImpl( 2L, 8),
                        new PointImpl( 3L, 1.2),
                        new PointImpl( 4L, 6.5)
                ));
        injector.addChunk(chunk1);
        ChunkVersionCurrent chunk2 = ChunkBuilderHelper.fromPoints("temp_a",
                Arrays.asList(
                        new PointImpl( 5L, -2),
                        new PointImpl( 6L, 8.8),
                        new PointImpl( 7L, 13.3),
                        new PointImpl( 8L, 2)
                ));
        injector.addChunk(chunk2);
        ChunkVersionCurrent chunk3 = ChunkBuilderHelper.fromPoints("temp_a",
                Arrays.asList(
                        new PointImpl( 9L, -5),
                        new PointImpl( 10L, 80),
                        new PointImpl( 11L, 1.2),
                        new PointImpl( 12L, 5.5)
                ));
        injector.addChunk(chunk3);
        ChunkVersionCurrent chunk4 = ChunkBuilderHelper.fromPoints("temp_b",
                Arrays.asList(
                        Arrays.asList(
                                new Point( 9L, -5),
                                new Point( 10L, 80),
                                new Point( 11L, 1.2),
                                new Point( 12L, 5.5)
                        )
                        new PointImpl( 9L, -5),
                        new PointImpl( 10L, 80),
                        new PointImpl( 11L, 1.2),
                        new PointImpl( 12L, 5.5)
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
        assertEquals("id", schemaRepresentation.getUniqueKey());
//        assertEquals(28, schemaRepresentation.getFields().size());
        assertEquals(69, schemaRepresentation.getDynamicFields().size());
        assertEquals(68, schemaRepresentation.getFieldTypes().size());
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
                        JsonArray docs = rsp.getJsonArray(HistorianServiceFields.CHUNKS);
                        assertEquals(4, docs.size());
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.NAME));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_START));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_END));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_AVG));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.ID));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_COUNT));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_SAX));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_MIN));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_TREND));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_SUM));
//                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_VERSION));
                        assertTrue(doc1.containsKey(HistorianChunkCollectionFieldsVersion0.CHUNK_FIRST));
                        assertEquals(18, doc1.size());
                        assertEquals("id0", doc1.getString("id"));
                        assertEquals(1L, doc1.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_START));
                        assertEquals(4L, doc1.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_END));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("id1", doc2.getString("id"));
                        assertEquals(5L, doc2.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_START));
                        assertEquals(8L, doc2.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_END));
                        JsonObject doc3 = docs.getJsonObject(2);
                        assertEquals("id2", doc3.getString("id"));
                        assertEquals(9L, doc3.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_START));
                        assertEquals(12L, doc3.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_END));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithStart(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(HistorianServiceFields.FROM, 9L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(HistorianServiceFields.CHUNKS);
                        JsonObject doc2 = docs.getJsonObject(0);
                        assertEquals("id2", doc2.getString("id"));
                        JsonObject doc3 = docs.getJsonObject(1);
                        assertEquals("id3", doc3.getString("id"));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithEnd(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(HistorianServiceFields.TO, 1571129390801L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(HistorianServiceFields.CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals("id0", doc1.getString("id"));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("id1", doc2.getString("id"));
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
                    .add(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE)
                        .add(HistorianChunkCollectionFieldsVersion0.CHUNK_START)
                        .add(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX).add("id")
                );
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(HistorianServiceFields.CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals(4, doc1.size());
                        assertEquals("id0", doc1.getString("id"));
                        assertEquals(1L, doc1.getLong(HistorianChunkCollectionFieldsVersion0.CHUNK_START));
                        assertEquals(8.0, doc1.getDouble(HistorianChunkCollectionFieldsVersion0.CHUNK_MAX));
                        assertEquals("H4sIAAAAAAAAAOPi1GQAAxEHLm4FRihHwYGLU9MYDD7bc3ELwMSlHAQYANb3vjkyAAAA",
                                doc1.getString(HistorianChunkCollectionFieldsVersion0.CHUNK_VALUE));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithName(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(HistorianServiceFields.NAMES, Arrays.asList("temp_a"));
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(HistorianServiceFields.TOTAL);
                        assertEquals(3, totalHit);
                        JsonArray docs = rsp.getJsonArray(HistorianServiceFields.CHUNKS);
                        assertEquals(3, docs.size());
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    private static void assertValidSchemaResponse(SolrResponseBase schemaResponse) {
        assertEquals(0, schemaResponse.getStatus(), "Response contained errors: " + schemaResponse.toString());
        assertNull(schemaResponse.getResponse().get("errors"), "Response contained errors: " + schemaResponse.toString());
    }

}

