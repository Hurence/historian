package com.hurence.webapiservice.http.api.ingestion.util;

import io.vertx.core.json.JsonArray;

import java.util.LinkedHashMap;

public class AllFilesReport {

    public JsonArray correctPoints;
    public JsonArray namesOfTooBigFiles;
    public LinkedHashMap<LinkedHashMap, Integer> numberOfFailedPointsPerMetric;

    public AllFilesReport() {
        this.correctPoints = new JsonArray();
        this.namesOfTooBigFiles = new JsonArray();
        this.numberOfFailedPointsPerMetric = new LinkedHashMap<>();
    }
}