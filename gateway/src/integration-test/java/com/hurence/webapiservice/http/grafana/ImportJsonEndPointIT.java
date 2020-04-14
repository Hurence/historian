package com.hurence.webapiservice.http.grafana;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ImportJsonEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(ImportJsonEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;
    private static AssertResponseGivenRequestHelper assertHelper1;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/query");
        assertHelper1 = new AssertResponseGivenRequestHelper(webClient, "/historian-server/ingestion/json");
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testNewAdd(Vertx vertx, VertxTestContext testContext) {

        assertAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testAdd/request.json",
                "/http/grafana/query/extract-algo/testAdd/expectedResponse.json");
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/test0/request.json",
                "/http/grafana/query/extract-algo/test0/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAddWithMissingNameField(Vertx vertx, VertxTestContext testContext) {

        assertMissingPointsAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingNameField/testAdd/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingNameField/testAdd/expectedResponse.json");
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingNameField/testQuery/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingNameField/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAddWithMissingPointsField(Vertx vertx, VertxTestContext testContext) {

        assertMissingPointsAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingPointsField/testAdd/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingPointsField/testAdd/expectedResponse.json");
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingPointsField/testQuery/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testMissingPointsField/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAddWithInvalidPointsSize(Vertx vertx, VertxTestContext testContext) {

        assertMissingPointsAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testInvalidPointsSize/testAdd/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testInvalidPointsSize/testAdd/expectedResponse.json");
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testInvalidPointsSize/testQuery/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testInvalidPointsSize/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAddWithNullDate(Vertx vertx, VertxTestContext testContext) {

        assertMissingPointsAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testNullDate/testAdd/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testNullDate/testAdd/expectedResponse.json");
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testNullDate/testQuery/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testNullDate/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAddWithNullValue(Vertx vertx, VertxTestContext testContext) {

        assertMissingPointsAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testNullValue/testAdd/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testNullValue/testAdd/expectedResponse.json");
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testMissingAdd/testNullValue/testQuery/request.json",
                "/http/grafana/query/extract-algo/testMissingAdd/testNullValue/testQuery/expectedResponse.json");
    }
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testWrongAddWithCode400(Vertx vertx, VertxTestContext testContext) {

        assertWrongAddRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testWrongAdd/request.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

    public void assertAddRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper1.assertRequestGiveObjectResponseFromFile(vertx, testContext, requestFile, responseFile);
    }
    public void assertWrongAddRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                     String requestFile) {
        assertHelper1.assertWrongRequestGiveResponseFromFile(vertx, testContext,
                requestFile);
    }
    public void assertMissingPointsAddRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                                  String requestFile, String responseFile) {
        assertHelper1.assertMissingPointsRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile);
    }
}
