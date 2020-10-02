package com.hurence.util;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import io.vertx.reactivex.ext.web.multipart.MultipartForm;

public class RequestResponseConf<RESPONSE> implements RequestResponseConfI<RESPONSE> {
    String endPointRequest;
    String requestFile;
    String responseFile;
    int statusCode;
    String statusMessage;
    BodyCodec<RESPONSE> responseType;
    final Vertx vertx;

    public RequestResponseConf(String endPointRequest,
                               String requestFile,
                               String responseFile,
                               int statusCode,
                               String statusMessage,
                               BodyCodec<RESPONSE> responseType,
                               Vertx vertx) {
        this.requestFile = requestFile;
        this.responseFile = responseFile;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
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
        final FileSystem fs = vertx.fileSystem();
        return fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(this.requestFile).getFile());
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
        return this.statusCode;
    }

    @Override
    public String getExpectedStatusMessage() {
        return this.statusMessage;
    }

    @Override
    public BodyCodec<RESPONSE> responseType() {
        return this.responseType;
    }

    @Override
    public boolean isMultipart() {
        return false;
    }

    @Override
    public MultipartForm getMultipartForm() {
        return null;
    }
}
