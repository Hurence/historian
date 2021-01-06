package com.hurence.util;

import com.hurence.webapiservice.http.api.ingestion.ImportCsvEndPointIT;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.client.HttpRequest;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertResponseGivenRequestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCsvEndPointIT.class);
    protected WebClient webClient;
    protected String endpoint;

    public AssertResponseGivenRequestHelper(WebClient webClient, String endpoint) {
        this.webClient = webClient;
        this.endpoint = endpoint;
    }

    public void assertRequestGiveArrayResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                       String requestFile, String responseFile) {
        assertRequestGiveArrayResponseFromFile(vertx, testContext, requestFile, responseFile,
                200, "OK");
    }


    public void assertRequestGiveArrayResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                       String requestFile, String responseFile,
                                                       int statusCode, String statusMessage) {
        RequestResponseConf<JsonArray> conf = new RequestResponseConf<>(endpoint, requestFile, responseFile,
                statusCode, statusMessage, BodyCodec.jsonArray(), vertx);
        assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, conf);
    }


    public void assertRequestGiveObjectResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                        String requestFile, String responseFile) {
        assertRequestGiveObjectResponseFromFile(vertx, testContext, requestFile, responseFile,
                200, "OK");
    }

    public void assertRequestGiveObjectResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                        String requestFile, String responseFile,
                                                        int statusCode, String statusMessage) {
        RequestResponseConf<JsonObject> conf = new RequestResponseConf<>(endpoint, requestFile, responseFile,
                statusCode, statusMessage, BodyCodec.jsonObject(), vertx);
        assertRequestGiveResponseFromFileAndFinishTest(webClient, testContext, conf);
    }

    public static <RESPONSE> Completable assertRequestGiveResponseFromFile(WebClient webClient,
                                                                           VertxTestContext testContext,
                                                                           RequestResponseConfI<RESPONSE> conf) {
        HttpRequest<RESPONSE> request = webClient
                .post(conf.getEndPointRequest())
                .as(conf.responseType());
        Single<HttpResponse<RESPONSE>> response;
        if (conf.isMultipart()) {
            response = request.rxSendMultipartForm(conf.getMultipartForm());
        } else {
            response = request.rxSendBuffer(conf.getRequestBody());
        }
        return response
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(conf.getExpectedStatusCode(), rsp.statusCode());
                        assertEquals(conf.getExpectedStatusMessage(), rsp.statusMessage());
                        RESPONSE body = rsp.body();
                        RESPONSE expectedBody = conf.getExpectedResponse();
                        assertEquals(expectedBody, body);
                    });
                })
                .doOnError(error -> {
                    /*If you get an error like
                    -----
                    io.vertx.core.json.DecodeException: Failed to decode: Unexpected character ('<' (code 60)):
                    expected a valid value (number, String, array, object, 'true', 'false' or 'null')
                        at [Source: (io.netty.buffer.ByteBufInputStream); line: 1, column: 2]
                    -----
                    This probably means there is a problem with the request file. Maybe your target folder it not up to date ?
                    Try rebuilding module and verify the file is in target.

                    Another thing can be that your endpoint has not been mounted on the router !
                    */
                    LOGGER.error("query to endpoint '" + conf.getEndPointRequest() + "' failed",error);
                    LOGGER.error("query to endpoint '{}' failed, isMultipartForm : {}\n request was {}\n expected response was {}",
                            conf.getEndPointRequest(),
                            conf.isMultipart(),
                            conf.getRequestBody().toString(),
                            conf.getExpectedResponse().toString());
                })
                .ignoreElement();
    }

    public static <T> void assertRequestGiveResponseFromFileAndFinishTest(WebClient webClient,
                                                                          VertxTestContext testContext,
                                                                          RequestResponseConf<T> conf) {
        assertRequestGiveResponseFromFile(webClient, testContext, conf)
                .subscribe(testContext::completeNow);
    }

    public static Completable assertRequestGiveResponseFromFile(WebClient webClient,
                                                                VertxTestContext testContext,
                                                                List<RequestResponseConfI<?>> confs) {
        Completable tests = null;
        for (RequestResponseConfI<?> conf : confs) {
            Completable test = assertRequestGiveResponseFromFile(webClient, testContext, conf);
            if (tests == null) {
                tests = test;
            } else {
                tests = tests.andThen(test);
            }
        }
        return  tests;
    }

    public static void assertRequestGiveResponseFromFileAndFinishTest(WebClient webClient,
                                                                      VertxTestContext testContext,
                                                                      List<RequestResponseConfI<?>> confs) {
        assertRequestGiveResponseFromFile(webClient, testContext, confs)
                .subscribe(() -> {
                            testContext.completeNow();
                        },error -> {
                            LOGGER.error("error in sery of request", error);
                            testContext.failNow(error);
                        });
    }
}
