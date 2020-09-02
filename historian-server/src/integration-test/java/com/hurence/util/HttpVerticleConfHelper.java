package com.hurence.util;

import com.hurence.webapiservice.http.HttpServerVerticle;
import io.vertx.core.json.JsonObject;

public class HttpVerticleConfHelper {

    public static void setMaxNumberOfDatapointAllowedInExport(JsonObject json, long max) {
        json.put(HttpServerVerticle.CONFIG_MAXDATAPOINT_MAXIMUM_ALLOWED, max);
    }


}
