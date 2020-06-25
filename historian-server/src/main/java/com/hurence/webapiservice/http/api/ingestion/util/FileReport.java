
package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;

import java.util.LinkedHashMap;

public class FileReport {

    public JsonArray correctPoints;
    public LinkedHashMap<LinkedHashMap, Integer> numberOfFailedPointsPerMetric;

    public FileReport() {
        this.correctPoints = new JsonArray();
        this.numberOfFailedPointsPerMetric = new LinkedHashMap<>();
    }
}