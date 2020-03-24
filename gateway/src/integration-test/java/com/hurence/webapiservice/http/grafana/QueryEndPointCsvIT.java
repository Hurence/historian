package com.hurence.webapiservice.http.grafana;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import com.hurence.historian.solr.injector.SolrInjector;
import com.hurence.historian.solr.injector.SolrInjectorMultipleMetricSpecificPoints;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
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

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointCsvIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, Vertx vertx) throws IOException, SolrServerException {
        HistorianSolrITHelper
                .initHistorianSolr(client);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        SolrInjector injector = new SolrInjectorMultipleMetricSpecificPoints(
                Arrays.asList("temp_a", "temp_b", "maxDataPoints"),
                Arrays.asList(
                        Arrays.asList(
                                new Point(0, 1477895624866L, 622),
                                new Point(0, 1477916224866L, -3),
                                new Point(0, 1477917224866L, 365)
                        ),
                        Arrays.asList(
                                new Point(0, 1477895624866L, 861),
                                new Point(0, 1477917224866L, 767)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                new Point(0, 1477895624866L, 1),
                                new Point(0, 1477895624867L, 1),
                                new Point(0, 1477895624868L, 1),
                                new Point(0, 1477895624869L, 1),
                                new Point(0, 1477895624870L, 1),
                                new Point(0, 1477895624871L, 1),
                                new Point(0, 1477895624872L, 1),
                                new Point(0, 1477895624873L, 1),
                                new Point(0, 1477895624874L, 1),
                                new Point(0, 1477895624875L, 1),
                                new Point(0, 1477895624876L, 1),
                                new Point(0, 1477895624877L, 1),
                                new Point(0, 1477895624878L, 1),
                                new Point(0, 1477895624879L, 1),
                                new Point(0, 1477895624880L, 1),
                                new Point(0, 1477895624881L, 1),
                                new Point(0, 1477895624882L, 1),
                                new Point(0, 1477895624883L, 1),
                                new Point(0, 1477895624884L, 1),
                                new Point(0, 1477895624885L, 1),
                                new Point(0, 1477895624886L, 1),
                                new Point(0, 1477895624887L, 1),
                                new Point(0, 1477895624888L, 1),
                                new Point(0, 1477895624889L, 1),
                                new Point(0, 1477895624890L, 1),
                                new Point(0, 1477895624891L, 1),
                                new Point(0, 1477895624892L, 1),
                                new Point(0, 1477895624893L, 1),
                                new Point(0, 1477895624894L, 1),
                                new Point(0, 1477895624895L, 1),
                                new Point(0, 1477895624896L, 1),
                                new Point(0, 1477895624897L, 1),
                                new Point(0, 1477895624898L, 1),
                                new Point(0, 1477895624899L, 1),
                                new Point(0, 1477895624900L, 1),
                                new Point(0, 1477895624901L, 1),
                                new Point(0, 1477895624902L, 1),
                                new Point(0, 1477895624903L, 1),
                                new Point(0, 1477895624904L, 1),
                                new Point(0, 1477895624905L, 1)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION_HISTORIAN);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/export/csv");
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
        int maxLimitFromConfig = 10000;
        HttpWithHistorianSolrITHelper.deployCustomHttpAndHistorianVerticle(container, vertx, maxLimitFromConfig)
                .map(t -> {
                    assertRequestGiveResponseFromFile(vertx, testContext,
                            "/http/grafana/query/extract-algo/test1/request.json",
                            "/http/grafana/query/extract-algo/test1/expectedResponse.csv");
                    return t;
                }).subscribe();
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testQueryWithMaxAllowedPointsPassed(DockerComposeContainer container, Vertx vertx, VertxTestContext testContext) {
        int maxLimitFromConfig = 100;
        HttpWithHistorianSolrITHelper.deployCustomHttpAndHistorianVerticle(container, vertx, maxLimitFromConfig)
                .map(t -> {
                    assertRequestGiveResponseFromFile(vertx, testContext,
                            "/http/grafana/query/extract-algo/test1/request.json");
                    return t;
                }).subscribe();
    }

    private void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext, String requestFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/export/csv")
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(413, rsp.statusCode());
                        assertEquals("max data points is bigger than allowed", rsp.statusMessage());
                        testContext.completeNow();
                    });
                }));
    }


    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post("/api/grafana/export/csv")
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
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
