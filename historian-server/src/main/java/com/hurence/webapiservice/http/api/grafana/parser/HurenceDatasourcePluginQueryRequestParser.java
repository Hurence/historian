package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.historian.modele.HistorianFields;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.modele.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static com.hurence.webapiservice.http.api.grafana.util.RequestParserUtil.*;
import static com.hurence.webapiservice.http.api.main.modele.QueryFields.QUERY_PARAM_REF_ID;

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
    private final String aggregationPath;
    private final String qualityPath;

    public HurenceDatasourcePluginQueryRequestParser(String fromJsonPath,
                                                     String toJsonPath,
                                                     String namesJsonPath,
                                                     String maxDatapointsJsonPath,
                                                     String formatJsonPath,
                                                     String tagsJsonPath,
                                                     String samplingAlgoJsonPath,
                                                     String bucketSizeJsonPath,
                                                     String requestIdJsonPath,
                                                     String aggregationPath,
                                                     String qualityPath) {
        this.fromJsonPath = fromJsonPath;
        this.toJsonPath = toJsonPath;
        this.namesJsonPath = namesJsonPath;
        this.maxDatapointsJsonPath = maxDatapointsJsonPath;
        this.formatJsonPath = formatJsonPath;
        this.tagsJsonPath = tagsJsonPath;
        this.samplingAlgoJsonPath = samplingAlgoJsonPath;
        this.bucketSizeJsonPath = bucketSizeJsonPath;
        this.requestIdJsonPath = requestIdJsonPath;
        this.aggregationPath= aggregationPath;
        this.qualityPath = qualityPath;
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
            builder.withTo(to);
        }
        String format = parseFormat(requestBody);
        if (format != null) {
            builder.withFormat(format);
        }
        JsonArray metricNames = parseMetricNames(requestBody);
        if (metricNames != null) {
            builder.withMetricNames(metricNames);
        } else {
            throw new IllegalArgumentException(String.format(
                    "request json should contain at least one name metric at path '%s'." +
                            "\nrequest received is %s", namesJsonPath, requestBody.encodePrettily()
            ));
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
            if (bucketSize <= 0) throw new IllegalArgumentException(String.format(
                    "request json should contain an integer > 0 for bucket size at path '%s'." +
                            "\nrequest received is %s", bucketSizeJsonPath, requestBody.encodePrettily()
            ));
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
        List<AGG> agreg = parseAggreg(requestBody);
        if (agreg != null) {
            builder.withAggreg(agreg);
        }
        Float quality = parseQuality(requestBody);
        if (quality != null) {
            builder.withQuality(quality);
        }
        return builder.build();
    }

    private Float parseQuality(JsonObject requestBody) {
        return parse(requestBody, qualityPath, Float.class);
    }

    private List<AGG> parseAggreg(JsonObject requestBody) {
        return parseListAGG(requestBody, aggregationPath);
    }

    private String parseRequestId(JsonObject requestBody) {
        return parseString(requestBody, requestIdJsonPath);
    }

    private Map<String, String> parseTags(JsonObject requestBody) {
        return parseMapStringString(requestBody, tagsJsonPath);
    }

    private Integer parseBucketSize(JsonObject requestBody) {
        return parse(requestBody, bucketSizeJsonPath, Integer.class);
    }

    private SamplingAlgorithm parseSamplingAlgo(JsonObject requestBody) {
        String algoStr  = parseString(requestBody, samplingAlgoJsonPath);
        if (algoStr == null) return null;
        return SamplingAlgorithm.valueOf(algoStr.toUpperCase());
    }

    private JsonArray parseMetricNames(JsonObject requestBody) {
        try {
            JsonPointer jsonPointer = JsonPointer.from(namesJsonPath);
            JsonArray jsonArray = (JsonArray) jsonPointer.queryJson(requestBody);
            if (jsonArray == null) return null;
            List<Object> jsonArrayAsList = jsonArray.stream()
                    .map(el -> {
                        if (el instanceof String) return el;
                        if (el instanceof JsonObject) {
                            JsonObject jsonObject = (JsonObject) el;
                            JsonObject toReturn = new JsonObject()
                                    .put(HistorianFields.NAME, jsonObject.getString(HistorianFields.NAME));
                            Map<String, String> tags = parseTags(jsonObject);
                            if (!tags.isEmpty())
                                toReturn.put(HistorianFields.TAGS, tags);
                            if (jsonObject.containsKey(QUERY_PARAM_REF_ID))
                                toReturn.put(QUERY_PARAM_REF_ID, jsonObject.getString(QUERY_PARAM_REF_ID));
                            return toReturn;
                        }
                        throw new IllegalArgumentException(String.format(
                                "field at path '%s' is not well formed. " +
                                        "Expected an array containing string or object with a name and optionnaly tags",
                                namesJsonPath));
                    }).collect(Collectors.toList());
            return new JsonArray(jsonArrayAsList);
        } catch (Exception ex) {
            LOGGER.error("error while decoding '" + namesJsonPath + "'",ex);
            throw new IllegalArgumentException(String.format(
                    "field at path '%s' is not well formed. " +
                            "Expected an array containing string or object with a name and optionnaly tags",
                    namesJsonPath), ex);
        }
    }

    private Long parseFrom(JsonObject requestBody) {
        return parseDate(requestBody, fromJsonPath);
    }

    private Long parseTo(JsonObject requestBody) {
        return parseDate(requestBody, toJsonPath);
    }

    private String parseFormat(JsonObject requestBody) {
        return parseString(requestBody, formatJsonPath);
    }

    private Integer parseMaxDataPoints(JsonObject requestBody) {
        return parse(requestBody, maxDatapointsJsonPath, Integer.class);
    }

}
