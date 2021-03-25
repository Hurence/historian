package com.hurence.webapiservice.http.api.grafana.parser;

import io.vertx.core.json.JsonObject;

public interface AbstractRequestParser<T> {
    JsonObject toJson(T request);

    T toRequest(JsonObject json);
}
