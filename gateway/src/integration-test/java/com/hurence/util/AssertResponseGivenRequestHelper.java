package com.hurence.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;

import static com.hurence.webapiservice.http.Codes.CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertResponseGivenRequestHelper {

    private WebClient webClient;
    private String endpoint;

    public AssertResponseGivenRequestHelper(WebClient webClient, String endpoint) {
        this.webClient = webClient;
        this.endpoint = endpoint;
    }

    public void assertRequestGiveArrayResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                       String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }

    public void assertRequestGiveObjectResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                        String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }

    public void assertWrongImportRequestWithBadRequestResponseGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                          String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(400, rsp.statusCode());
                        assertEquals("BAD REQUEST", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }
    public void assertWrongImportRequestWithOKResponseGiveArrayResponseFromFile (Vertx vertx, VertxTestContext testContext,
                      String addRequestFile, String addResponseFile,String queryRequestFile, String queryResponseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer addRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(addRequestFile).getFile());
        Buffer queryRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(queryRequestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(addRequestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(CREATED, rsp.statusCode());
                        assertEquals("Created", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(addResponseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                    });
                    webClient.post("/api/grafana/query")
                            .as(BodyCodec.jsonArray())
                            .sendBuffer(queryRequestBuffer.getDelegate(), testContext.succeeding(rspQuery -> {
                                testContext.verify(() -> {
                                    assertEquals(200, rspQuery.statusCode());
                                    assertEquals("OK", rspQuery.statusMessage());
                                    JsonArray body = rspQuery.body();
                                    Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(queryResponseFile).getFile());
                                    JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                                    assertEquals(expectedBody, body);
                                    testContext.completeNow();
                                });
                            }));
                }));
    }
    public void assertCorrectPointsRequestGiveArrayResponseFromFile (Vertx vertx, VertxTestContext testContext,
                                                                     String addRequestFile, String addResponseFile,String queryRequestFile, String queryResponseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer addRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(addRequestFile).getFile());
        Buffer queryRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(queryRequestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(addRequestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(addResponseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                    });
                    webClient.post("/api/grafana/query")
                            .as(BodyCodec.jsonArray())
                            .sendBuffer(queryRequestBuffer.getDelegate(), testContext.succeeding(rspQuery -> {
                                testContext.verify(() -> {
                                    assertEquals(200, rspQuery.statusCode());
                                    assertEquals("OK", rspQuery.statusMessage());
                                    JsonArray body = rspQuery.body();
                                    Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(queryResponseFile).getFile());
                                    JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                                    assertEquals(expectedBody, body);
                                    testContext.completeNow();
                                });
                            }));
                }));
    }

}
