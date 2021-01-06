package com.hurence.util;


import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import io.vertx.reactivex.ext.web.multipart.MultipartForm;

public interface RequestResponseConfI<RESPONSE> {
    //REQUEST
    String getEndPointRequest();
    Buffer getRequestBody();
    //RESPONSE
    RESPONSE getExpectedResponse();
    int getExpectedStatusCode();
    String getExpectedStatusMessage();
    BodyCodec<RESPONSE> responseType();
    boolean isMultipart();
    MultipartForm getMultipartForm();
}
