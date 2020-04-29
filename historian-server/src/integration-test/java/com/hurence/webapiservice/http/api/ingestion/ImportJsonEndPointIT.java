package com.hurence.webapiservice.http.api.ingestion;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.util.RequestResponseConf;
import com.hurence.util.RequestResponseConfI;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.http.StatusCodes.CREATED;
import static com.hurence.webapiservice.http.StatusCodes.OK;
import static com.hurence.webapiservice.http.HttpServerVerticle.IMPORT_JSON_ENDPOINT;
import static com.hurence.webapiservice.http.HttpServerVerticle.GRAFANA_QUERY_ENDPOINT;


@ExtendWith({VertxExtension.class, SolrExtension.class})
public class ImportJsonEndPointIT {

    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper1;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper1 = new AssertResponseGivenRequestHelper(webClient, IMPORT_JSON_ENDPOINT);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testCorrectJsonImport(Vertx vertx, VertxTestContext testContext) {

        assertCorrectPointsImportRequest(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testCorrectJsonImport/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testCorrectJsonImport/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testCorrectJsonImport/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testCorrectJsonImport/testQuery/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithMissingNameField(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithBadRequestResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithMissingNameField/request.json",
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithMissingNameField/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNoStringNameField(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithBadRequestResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithNoStringNameField/request.json",
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithNoStringNameField/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNullNameField(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullNameField/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullNameField/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullNameField/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullNameField/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithMissingPointsField(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithBadRequestResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithMissingPointsField/request.json",
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithMissingPointsField/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNonJsonArrayPointsField(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithBadRequestResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithNonJsonArrayPointsField/request.json",
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithNonJsonArrayPointsField/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithZeroPointsSize(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithZeroPointsSize/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithZeroPointsSize/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithZeroPointsSize/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithZeroPointsSize/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNoTwoOrZeroPointsSize(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithBadRequestResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithNoTwoOrZeroPointsSize/request.json",
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportWithNoTwoOrZeroPointsSize/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNullDate(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullDate/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullDate/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullDate/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullDate/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNoLongDate(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoLongDate/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoLongDate/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoLongDate/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoLongDate/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNoJsonArrayPoint(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoJsonArrayPoint/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoJsonArrayPoint/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoJsonArrayPoint/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoJsonArrayPoint/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNullPoint(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullPoint/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullPoint/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullPoint/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullPoint/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNullValue(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullValue/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullValue/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullValue/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNullValue/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportWithNoDoubleValue(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithOKResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoDoubleValue/testImportJson/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoDoubleValue/testImportJson/expectedResponse.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoDoubleValue/testQuery/request.json",
                "/http/ingestion/importjson/testsImportJsonWithStatusOK/testImportWithNoDoubleValue/testQuery/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testImportAllNonBadRequestErrors(Vertx vertx, VertxTestContext testContext) {

        assertWrongImportRequestWithBadRequestResponse(vertx, testContext,
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportAllNonBadRequestErrors/request.json",
                "/http/ingestion/importjson/testsImportJsonWithResponseBadRequest/testImportAllNonBadRequestErrors/expectedResponse.json");
    }

    public void assertWrongImportRequestWithBadRequestResponse(Vertx vertx, VertxTestContext testContext,
                                                               String requestFile, String responseFile) {
        assertHelper1.assertRequestGiveObjectResponseFromFile(vertx, testContext,
                requestFile, responseFile, 400, "BAD REQUEST");
    }

    public void assertWrongImportRequestWithOKResponse(Vertx vertx, VertxTestContext testContext,
                                                       String addRequestFile, String addResponseFile, String queryRequestFile, String queryResponseFile) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonObject>(IMPORT_JSON_ENDPOINT, addRequestFile, addResponseFile, CREATED, "Created", BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(GRAFANA_QUERY_ENDPOINT, queryRequestFile, queryResponseFile, OK, "OK", BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }

    public void assertCorrectPointsImportRequest(Vertx vertx, VertxTestContext testContext,
                                                 String addRequestFile, String addResponseFile, String queryRequestFile, String queryResponseFile) {
        List<RequestResponseConfI<?>> confs = Arrays.asList(
                new RequestResponseConf<JsonObject>(IMPORT_JSON_ENDPOINT, addRequestFile, addResponseFile, OK, "OK", BodyCodec.jsonObject(), vertx),
                new RequestResponseConf<JsonArray>(GRAFANA_QUERY_ENDPOINT, queryRequestFile, queryResponseFile, OK, "OK", BodyCodec.jsonArray(), vertx)
        );
        AssertResponseGivenRequestHelper
                .assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, confs);
    }
}
