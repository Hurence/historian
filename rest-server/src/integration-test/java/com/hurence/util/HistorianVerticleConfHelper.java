package com.hurence.util;

import com.hurence.historian.model.SchemaVersion;
import com.hurence.webapiservice.historian.HistorianVerticle;
import io.vertx.core.json.JsonObject;

public class HistorianVerticleConfHelper {

    public static void setSchemaVersion(JsonObject json, SchemaVersion schema) {
        json.put(HistorianVerticle.CONFIG_SCHEMA_VERSION, schema.toString());
    }


}
