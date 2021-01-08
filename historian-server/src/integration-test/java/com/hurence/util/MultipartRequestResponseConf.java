package com.hurence.util;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import io.vertx.reactivex.ext.web.multipart.MultipartForm;

public class MultipartRequestResponseConf<RESPONSE> implements RequestResponseConfI<RESPONSE> {
    MultipartForm multipartForm;
    String endPointRequest;
    String responseFile;
    int expectedStatusCode;
    String expectedStatusMessage;
    BodyCodec<RESPONSE> responseType;
    final Vertx vertx;

    public MultipartRequestResponseConf(String endPointRequest,
                                        MultipartForm multipartForm,
                                        String responseFile,
                                        int expectedStatusCode,
                                        String expectedStatusMessage,
                                        BodyCodec<RESPONSE> responseType,
                                        Vertx vertx) {
        this.multipartForm = multipartForm;
        this.responseFile = responseFile;
        this.expectedStatusCode = expectedStatusCode;
        this.expectedStatusMessage = expectedStatusMessage;
        this.responseType = responseType;
        this.endPointRequest = endPointRequest;
        this.vertx = vertx;
    }

    @Override
    public String getEndPointRequest() {
        return this.endPointRequest;
    }

    @Override
    public Buffer getRequestBody() {
        return null;
    }

    @Override
    public RESPONSE getExpectedResponse() {
        final FileSystem fs = vertx.fileSystem();
        Buffer buffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(this.responseFile).getFile());
        if (responseType.equals(BodyCodec.jsonObject())) {
            return (RESPONSE)new JsonObject(buffer.getDelegate());
        } else if (responseType.equals(BodyCodec.jsonArray())) {
            return (RESPONSE)new JsonArray(buffer.getDelegate());
        } else {
            throw new IllegalArgumentException("unsupported response type !");
        }
    }

    @Override
    public int getExpectedStatusCode() {
        return this.expectedStatusCode;
    }

    @Override
    public String getExpectedStatusMessage() {
        return this.expectedStatusMessage;
    }

    @Override
    public BodyCodec<RESPONSE> responseType() {
        return this.responseType;
    }

    @Override
    public boolean isMultipart() {
        return true;
    }

    @Override
    public MultipartForm getMultipartForm() {
        return this.multipartForm;
    }
}
