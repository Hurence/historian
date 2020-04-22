package com.hurence.util;

import io.reactivex.Completable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertResponseGivenRequestHelper {

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
                statusCode, statusMessage, BodyCodec.jsonArray());
        assertRequestGiveResponseFromFileAndFinishTest(webClient, vertx, testContext, conf);
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
                statusCode, statusMessage, BodyCodec.jsonObject());
        assertRequestGiveResponseFromFileAndFinishTest(webClient, vertx, testContext, conf);
    }

    public static <T> Completable assertRequestGiveResponseFromFile(WebClient webClient, Vertx vertx,
                                                                    VertxTestContext testContext,
                                                                    RequestResponseConf<T> conf) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(conf.requestFile).getFile());
        return webClient.post(conf.endPointRequest)
                .as(conf.responseType)
                .rxSendBuffer(requestBuffer)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(conf.statusCode, rsp.statusCode());
                        assertEquals(conf.statusMessage, rsp.statusMessage());
                        T body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(conf.responseFile).getFile());
                        T expectedBody;
                        if (body instanceof JsonObject) {
                            expectedBody = (T)new JsonObject(fileContent.getDelegate());
                        } else if (body instanceof JsonArray) {
                            expectedBody = (T)new JsonArray(fileContent.getDelegate());
                        } else {
                            throw new IllegalArgumentException("unsupported response type !");
                        }
                        assertEquals(expectedBody, body);
                    });
                }).ignoreElement();
    }

    public static <T> void assertRequestGiveResponseFromFileAndFinishTest(WebClient webClient, Vertx vertx,
                                                                          VertxTestContext testContext,
                                                                          RequestResponseConf<T> conf) {
        assertRequestGiveResponseFromFile(webClient, vertx, testContext, conf)
                .subscribe(testContext::completeNow);
    }

    public static Completable assertRequestGiveResponseFromFile(WebClient webClient, Vertx vertx,
                                                                          VertxTestContext testContext,
                                                                          List<RequestResponseConf<?>> confs) {
        Completable tests = null;
        for (RequestResponseConf conf : confs) {
            Completable test = assertRequestGiveResponseFromFile(webClient, vertx, testContext, conf);
            if (tests == null) {
                tests = test;
            } else {
                tests = tests.andThen(test);
            }
        }
        return  tests;
    }

    public static void assertRequestGiveResponseFromFileAndFinishTest(WebClient webClient, Vertx vertx,
                                                                    VertxTestContext testContext,
                                                                    List<RequestResponseConf<?>> confs) {
        assertRequestGiveResponseFromFile(webClient, vertx, testContext, confs)
                .subscribe(testContext::completeNow);
    }
}
