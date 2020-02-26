package com.hurence.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hurence.webapiservice.historian.HistorianFields.RESPONSE_ANNOTATIONS;
import static com.hurence.webapiservice.historian.HistorianFields.RESPONSE_TOTAL_FOUND;
import static org.junit.jupiter.api.Assertions.*;

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

    public void assertRequestGiveObjectResponseFromFileWithNoOrder(Vertx vertx, VertxTestContext testContext,
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
                        int totalExpected = expectedBody.getInteger(RESPONSE_TOTAL_FOUND);
                        int total = body.getInteger(RESPONSE_TOTAL_FOUND);
                        JsonArray annotationExpected = expectedBody.getJsonArray(RESPONSE_ANNOTATIONS);
                        JsonArray annotations = body.getJsonArray(RESPONSE_ANNOTATIONS);
                        boolean isEqual = true;
                        for (Object je1 : annotations) {
                            boolean flag = false;
                            for(Object je2 : annotationExpected){
                                flag = je1.equals(je2);
                                if(flag){
                                    break;
                                }
                            }
                            isEqual = isEqual && flag;
                        }
                        assertTrue(isEqual);
                        assertEquals(totalExpected, total);
                        testContext.completeNow();
                    });
                }));
    }

}
