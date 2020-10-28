package com.hurence.webapiservice.http.api.main;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion;
import com.hurence.timeseries.model.Measure;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.HistorianVerticleConfHelper;
import com.hurence.util.HttpVerticleConfHelper;
import com.hurence.webapiservice.http.HttpServerVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.ext.web.client.WebClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.http.api.modele.StatusMessages.BAD_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ExportCsvEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(ExportCsvEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;
    private static String exportEndpoint =  HttpServerVerticle.CSV_EXPORT_ENDPOINT;

    @BeforeAll
    public static void beforeAll(SolrClient client, Vertx vertx, DockerComposeContainer container) throws IOException, SolrServerException, InterruptedException {
        HistorianSolrITHelper.createChunkCollection(client, container, SchemaVersion.getCurrentVersion());
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new SolrInjectorMultipleMetricSpecificPointsChunkCurrentVersion(
                Arrays.asList("temp_a", "temp_b"),
                Arrays.asList(
                        Arrays.asList(
                                Measure.fromValueAndQuality( 1477895624866L, 622.1, 0.9f),
                                Measure.fromValue( 1477916224866L, -3),
                                Measure.fromValue( 1477917224866L, 365)
                        ),
                        Arrays.asList(
                                Measure.fromValue( 1477895624866L, 861),
                                Measure.fromValueAndQuality( 1477917224866L, 767, 0.2f)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, exportEndpoint);
    }

    @AfterAll
    public static void afterAll(Vertx vertx) {
        webClient.close();
        vertx.close();
    }
    @AfterEach
    public void afterEach(Vertx vertx) {
        vertx.deploymentIDs().forEach(vertx::undeploy);
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryExportCsv(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject httpConf = new JsonObject();
        HttpVerticleConfHelper.setMaxNumberOfDatapointAllowedInExport(httpConf, 10000);
        JsonObject historianConf = new JsonObject();
        HistorianVerticleConfHelper.setSchemaVersion(historianConf, SchemaVersion.VERSION_1);
        HttpWithHistorianSolrITHelper.deployCustomHttpAndCustomHistorianVerticle(container, vertx, historianConf, httpConf)
                .map(t -> {
                    assertRequestGiveResponseFromFile(vertx, testContext,
                            "/http/mainapi/test1/request.json",
                            "/http/mainapi/test1/expectedResponse.csv");
                    return t;
                }).subscribe();
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithMaxAllowedPointsPassed(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject httpConf = new JsonObject();
        HttpVerticleConfHelper.setMaxNumberOfDatapointAllowedInExport(httpConf, 100);
        JsonObject historianConf = new JsonObject();
        HistorianVerticleConfHelper.setSchemaVersion(historianConf, SchemaVersion.VERSION_1);
        HttpWithHistorianSolrITHelper.deployCustomHttpAndCustomHistorianVerticle(container, vertx, historianConf, httpConf)
                .map(t -> {
                    assertRequestGiveResponseFromFile(vertx, testContext,
                            "/http/mainapi/test1/request.json");
                    return t;
                }).subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryExportCsvWithQuality(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        JsonObject httpConf = new JsonObject();
        HttpVerticleConfHelper.setMaxNumberOfDatapointAllowedInExport(httpConf, 10000);
        JsonObject historianConf = new JsonObject();
        HistorianVerticleConfHelper.setSchemaVersion(historianConf, SchemaVersion.VERSION_1);
        HttpWithHistorianSolrITHelper.deployCustomHttpAndCustomHistorianVerticle(container, vertx, historianConf, httpConf)
                .map(t -> {
                    assertRequestGiveResponseFromFile(vertx, testContext,
                            "/http/mainapi/test1/request-with-quality.json",
                            "/http/mainapi/test1/expectedResponse.csv");
                    return t;
                }).subscribe();
    }

    private void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext, String requestFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(exportEndpoint)
                .sendBuffer(requestBuffer, testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(413, rsp.statusCode());
                        assertEquals(BAD_REQUEST, rsp.statusMessage());
                        assertEquals(new JsonObject("{\n" +
                                "  \"Error Message\" : \"max data measures is bigger than allowed\"\n" +
                                "}"), rsp.bodyAsJsonObject());
                        testContext.completeNow();
                    });
                }));
    }


    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(exportEndpoint)
                .sendBuffer(requestBuffer, testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        String body = rsp.body().toString();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        String expectedBody = fileContent.toString();
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }

}
