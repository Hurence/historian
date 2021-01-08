package com.hurence.webapiservice.http.api.analytics.model;

import io.vertx.core.json.JsonObject;
import org.joda.time.DateTime;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static com.hurence.historian.model.HistorianServiceFields.*;


public class ClusteringRequest {
    String day;
    String queryFilter;

    String algorithm = "kmeans";
    int k = 3;
    int maxIterations = 100;
    List<String> names = new ArrayList<>();

    public String getDay() {
        return day;
    }

    public String getQueryFilter() {
        return queryFilter;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public int getK() {
        return k;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public JsonObject toParams() {
        JsonObject params = new JsonObject();

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dt = formatter.parseDateTime(day + " 00:00:00");

        params.put(FROM, dt.getMillis());
        params.put(TO, dt.plusDays(1).getMillis());
        params.put(NAMES, names);

        return params;
    }


    public static ClusteringRequest fromJson(JsonObject json) {
        ClusteringRequest request = new ClusteringRequest();
        try {
            DateTimeZone timeZone = DateTimeZone.forID("UTC");
            DateTime time = new DateTime(timeZone);
            String today = time.toString("yyyy-MM-dd");


            request.k = json.getInteger("k") != null ? json.getInteger("k") : 3;
            request.maxIterations = json.getInteger("maxIterations") != null ? json.getInteger("maxIterations") : 100;
            request.algorithm = json.getString("algorithm") != null ? json.getString("algorithm") : "kmeans";
            request.day = json.getString("day") != null ? json.getString("day") : today;
            request.algorithm = json.getString("algorithm") != null ? json.getString("algorithm") : "kmeans";
            request.queryFilter = json.getString("queryFilter") != null ? json.getString("queryFilter") : "";
            request.names = json.getJsonArray(NAMES) != null ?
                    json.getJsonArray(NAMES).getList() :
                    new ArrayList<>();

        } catch (Exception ex) {
            throw new IllegalArgumentException("unable to parse ClusteringRequest");
        }
        return request;


    }
}
