package com.hurence.webapiservice.http.api.main;

import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.model.AdHocFilter;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.api.grafana.util.RequestParserUtil.parseDate;
import static com.hurence.webapiservice.http.api.main.modele.QueryFields.*;

public class GetTimeSerieJsonRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetTimeSerieJsonRequestParser.class);
    /*
      REST API PARAMS
     */

    public GetTimeSerieRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        GetTimeSerieRequestParam.Builder builder = new GetTimeSerieRequestParam.Builder();
        Long from = parseFrom(requestBody);
        builder.from(from == null ? GetTimeSerieRequestParam.DEFAULT_FROM : from);
        Long to = parseTo(requestBody);
        builder.to(to == null ? GetTimeSerieRequestParam.DEFAULT_TO : to);
        List<String> metrics = parseMetrics(requestBody);
        builder.withMetricNames(metrics);
        Integer maxDataPoints = parseMaxDataPoints(requestBody);;
        builder.withMaxDataPoints(maxDataPoints == null ? GetTimeSerieRequestParam.DEFAULT_MAX_DATAPOINTS : maxDataPoints);
        Integer bucketSize = parseBucketSize(requestBody);
        builder.withBucketSize(bucketSize == null ? GetTimeSerieRequestParam.DEFAULT_BUCKET_SIZE : bucketSize);
        SamplingAlgorithm samplingAlgo = parseSamplingAlgo(requestBody);
        builder.withSamplingAlgo(samplingAlgo == null ? GetTimeSerieRequestParam.DEFAULT_SAMPLING_ALGORITHM : samplingAlgo);
        List<AGG> aggs = parseAggs(requestBody);
        builder.withAggs(aggs == null ? GetTimeSerieRequestParam.DEFAULT_AGGS : aggs);
        Map<String,String> tags = parseTags(requestBody);
        builder.withTags(tags);
        return builder.build();
    }

    private SamplingAlgorithm parseSamplingAlgo(JsonObject requestBody) {
        String algo = requestBody.getString(QUERY_PARAM_SAMPLING);
        if (algo != null) return SamplingAlgorithm.valueOf(algo);
        return null;
    }

    private Map<String, String> parseTags(JsonObject requestBody) {
        final Map<String, String> tags = new HashMap<>();
        JsonObject tagsJson = requestBody.getJsonObject(QUERY_PARAM_TAGS);
        if (tagsJson != null) {
            tagsJson.stream()
                    .forEach(mapEntry -> {
                        tags.put(mapEntry.getKey(), (String) mapEntry.getValue());
                    });
        }
        return tags;
    }

    private List<AGG> parseAggs(JsonObject requestBody) {
        JsonArray aggs = requestBody.getJsonArray(QUERY_PARAM_AGGS);
        if (aggs == null) return null;
        return aggs.stream()
                .map(String.class::cast)
                .map(AGG::valueOf)
                .collect(Collectors.toList());
    }

    private List<String> parseMetrics(JsonObject requestBody) {
        return requestBody.getJsonArray(QUERY_PARAM_NAME).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    private Long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, "/" + QUERY_PARAM_FROM);
    }

    private Long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, "/" + QUERY_PARAM_TO);
    }

    private Integer parseMaxDataPoints(JsonObject requestBody) {
        return requestBody.getInteger(QUERY_PARAM_MAX_POINT);
    }

    private Integer parseBucketSize(JsonObject requestBody) {
        return requestBody.getInteger(QUERY_PARAM_BUCKET_SIZE);
    }

    private List<AdHocFilter> parseAdHocFilters(JsonObject requestBody) {
        if (!requestBody.containsKey("adhocFilters"))
            return Collections.emptyList();
        return requestBody.getJsonArray("adhocFilters").stream()
                .map(JsonObject.class::cast)
                .map(JsonObject::encode)
                .map(json -> Json.decodeValue(json, AdHocFilter.class))
                .collect(Collectors.toList());
    }
}
