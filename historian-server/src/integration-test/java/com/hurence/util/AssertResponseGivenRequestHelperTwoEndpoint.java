package com.hurence.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import junit.framework.Assert;

import static com.hurence.webapiservice.http.Codes.CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertResponseGivenRequestHelperTwoEndpoint extends AssertResponseGivenRequestHelper {

    protected String endpoint2;

    public AssertResponseGivenRequestHelperTwoEndpoint(WebClient webClient, String endpoint, String endpoint2) {
        super(webClient, endpoint);
        this.endpoint2 = endpoint2;
    }

    //TODO simplify those methods (factorize)
    public void assertWrongImportRequestWithOKResponseGiveArrayResponseFromFile (Vertx vertx, VertxTestContext testContext,
                      String addRequestFile, String addResponseFile,String queryRequestFile, String queryResponseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer addRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(addRequestFile).getFile());
        Buffer queryRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(queryRequestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(addRequestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(CREATED, rsp.statusCode());
                        assertEquals("Created", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(addResponseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                    });
                    webClient.post(endpoint2)
                            .as(BodyCodec.jsonArray())
                            .sendBuffer(queryRequestBuffer.getDelegate(), testContext.succeeding(rspQuery -> {
                                testContext.verify(() -> {
                                    assertEquals(200, rspQuery.statusCode());
                                    assertEquals("OK", rspQuery.statusMessage());
                                    JsonArray body = rspQuery.body();
                                    Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(queryResponseFile).getFile());
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
        Buffer addRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(addRequestFile).getFile());
        Buffer queryRequestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(queryRequestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(addRequestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(addResponseFile).getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                    });
                    webClient.post(endpoint2)
                            .as(BodyCodec.jsonArray())
                            .sendBuffer(queryRequestBuffer.getDelegate(), testContext.succeeding(rspQuery -> {
                                testContext.verify(() -> {
                                    assertEquals(200, rspQuery.statusCode());
                                    assertEquals("OK", rspQuery.statusMessage());
                                    JsonArray body = rspQuery.body();
                                    Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelperTwoEndpoint.class.getResource(queryResponseFile).getFile());
                                    JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                                    assertEquals(expectedBody, body);
                                    testContext.completeNow();
                                });
                            }));
                }));
    }

}
