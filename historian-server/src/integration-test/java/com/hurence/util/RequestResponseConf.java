package com.hurence.util;


import io.vertx.reactivex.ext.web.codec.BodyCodec;

public class RequestResponseConf<T> {
    String endPointRequest;
    String requestFile;
    String responseFile;
    int statusCode;
    String statusMessage;
    BodyCodec<T> responseType;

    public RequestResponseConf(String endPointRequest, String requestFile, String responseFile, int statusCode, String statusMessage, BodyCodec<T> responseType) {
        this.requestFile = requestFile;
        this.responseFile = responseFile;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.responseType = responseType;
        this.endPointRequest = endPointRequest;
    }
}
