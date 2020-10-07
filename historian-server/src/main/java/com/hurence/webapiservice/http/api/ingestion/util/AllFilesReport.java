package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;

import java.util.LinkedHashMap;

public class AllFilesReport {

    public JsonArray correctPoints;
    public JsonArray filesThatFailedToBeImported;
    public LinkedHashMap<LinkedHashMap<String,String>, Integer> numberOfFailedPointsPerMetric;

    public AllFilesReport() {
        this.correctPoints = new JsonArray();
        this.filesThatFailedToBeImported = new JsonArray();
        this.numberOfFailedPointsPerMetric = new LinkedHashMap<>();
    }
}