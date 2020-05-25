package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.modele.HurenceDatasourcePluginQueryRequestParam;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.http.api.grafana.util.RequestParserUtil.parseDate;
import static com.hurence.webapiservice.http.api.grafana.util.RequestParserUtil.parseMapStringString;

public class HurenceDatasourcePluginQueryRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(HurenceDatasourcePluginQueryRequestParser.class);

    private final String fromJsonPath;
    private final String toJsonPath;
    private final String namesJsonPath;
    private final String maxDatapointsJsonPath;
    private final String formatJsonPath;
    private final String tagsJsonPath;
    private final String samplingAlgoJsonPath;
    private final String bucketSizeJsonPath;
    private final String requestIdJsonPath;

    public HurenceDatasourcePluginQueryRequestParser(String fromJsonPath,
                                                     String toJsonPath,
                                                     String namesJsonPath,
                                                     String maxDatapointsJsonPath,
                                                     String formatJsonPath,
                                                     String tagsJsonPath,
                                                     String samplingAlgoJsonPath,
                                                     String bucketSizeJsonPath,
                                                     String requestIdJsonPath) {
        this.fromJsonPath = fromJsonPath;
        this.toJsonPath = toJsonPath;
        this.namesJsonPath = namesJsonPath;
        this.maxDatapointsJsonPath = maxDatapointsJsonPath;
        this.formatJsonPath = formatJsonPath;
        this.tagsJsonPath = tagsJsonPath;
        this.samplingAlgoJsonPath = samplingAlgoJsonPath;
        this.bucketSizeJsonPath = bucketSizeJsonPath;
        this.requestIdJsonPath = requestIdJsonPath;
    }

    public HurenceDatasourcePluginQueryRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse requestBody : {}", requestBody);
        HurenceDatasourcePluginQueryRequestParam.Builder builder = new HurenceDatasourcePluginQueryRequestParam.Builder();
        Long from = parseFrom(requestBody);
        if (from != null) {
            builder.withFrom(from);
        }
        Long to = parseTo(requestBody);
        if (to != null) {
            builder.withFrom(to);
        }
        String format = parseFormat(requestBody);
        if (format != null) {
            builder.withFormat(format);
        }
        List<String> metricNames = parseMetricNames(requestBody);
        if (metricNames != null) {
            builder.withMetricNames(metricNames);
        }
        Integer maxDataPoints = parseMaxDataPoints(requestBody);
        if (maxDataPoints != null) {
            builder.withMaxDataPoints(maxDataPoints);
        }
        SamplingAlgorithm samplingAlgo= parseSamplingAlgo(requestBody);
        if (samplingAlgo != null) {
            builder.withSamplingAlgo(samplingAlgo);
        }
        Integer bucketSize = parseBucketSize(requestBody);
        if (bucketSize != null) {
            builder.withBucketSize(bucketSize);
        }
        Map<String, String> tags = parseTags(requestBody);
        if (tags != null) {
            builder.withTags(tags);
        }
        String requestId = parseRequestId(requestBody);
        if (requestId != null) {
            builder.withRequestId(requestId);
        }
        return builder.build();
    }

    private String parseRequestId(JsonObject requestBody) {
        return requestBody.getString(requestIdJsonPath);
    }

    private Map<String, String> parseTags(JsonObject requestBody) {
        return parseMapStringString(requestBody, tagsJsonPath);
    }

    private Integer parseBucketSize(JsonObject requestBody) {
        return requestBody.getInteger(bucketSizeJsonPath);
    }

    private SamplingAlgorithm parseSamplingAlgo(JsonObject requestBody) {
        String algoStr  = requestBody.getString(samplingAlgoJsonPath);
        return SamplingAlgorithm.valueOf(algoStr);
    }

    private List<String> parseMetricNames(JsonObject requestBody) {
        return requestBody.getJsonArray(namesJsonPath).stream()
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    private Long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, fromJsonPath);
    }

    private Long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, toJsonPath);
    }

    private String parseFormat(JsonObject requestBody) {
        return requestBody.getString(formatJsonPath);
    }

    private Integer parseMaxDataPoints(JsonObject requestBody) {
        return requestBody.getInteger(maxDatapointsJsonPath);
    }

}
