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

import static org.junit.jupiter.api.Assertions.*;

public class AssertResponseGivenRequestHelper {

    private WebClient webClient;
    private String endpoint;

    public AssertResponseGivenRequestHelper(WebClient webClient, String endpoint) {
        this.webClient = webClient;
        this.endpoint = endpoint;
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                         String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body().getJsonArray("annotations");
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }
    public void assertRequestGiveResponseFromFileWithNoOrder(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body().getJsonArray("annotations");
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        boolean isEqual = true;
                        for (Object je1 : body) {
                            boolean flag = false;
                            for(Object je2 : expectedBody){
                                flag = je1.equals(je2);
                                if(flag){
                                    break;
                                }
                            }
                            isEqual = isEqual && flag;
                        }
                        assertTrue(isEqual);
                        testContext.completeNow();
                    });
                }));
    }
    public void assertRequestGiveResponseFromFileWithLimit(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonObject())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body().getJsonArray("annotations");
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody.size(), body.size());
                        testContext.completeNow();
                    });
                }));
    }
}
