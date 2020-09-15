package com.hurence.util;

import com.hurence.historian.modele.SchemaVersion;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.vertx.core.json.JsonObject;

public class HistorianVerticleConfHelper {

    public static void setSchemaVersion(JsonObject json, SchemaVersion schema) {
        json.put(HistorianVerticle.CONFIG_SCHEMA_VERSION, schema.toString());
    }


}
