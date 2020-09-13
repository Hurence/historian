package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;

import java.util.LinkedHashMap;

public class AllFilesReport {

    public JsonArray correctPoints;
    public JsonArray failedFiles;
    public LinkedHashMap<LinkedHashMap<String,String>, Integer> numberOfFailedPointsPerMetric;

    public AllFilesReport() {
        this.correctPoints = new JsonArray();
        this.failedFiles = new JsonArray();
        this.numberOfFailedPointsPerMetric = new LinkedHashMap<>();
    }
}