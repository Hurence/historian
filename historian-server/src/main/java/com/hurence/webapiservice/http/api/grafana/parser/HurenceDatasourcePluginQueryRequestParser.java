package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.model.HurenceDatasourcePluginQueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.util.QualityAgg;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.json.pointer.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.hurence.historian.model.HistorianServiceFields.QUALITY_VALUE;
import static com.hurence.timeseries.model.Definitions.FIELD_NAME;
import static com.hurence.timeseries.model.Definitions.FIELD_TAGS;
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
    private final String qualityValuePath;
    private final String qualityAggPath;
    private final String qualityReturnPath;


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
                                                     String qualityValuePath,
                                                     String qualityAggPath,
                                                     String qualityReturnPath) {
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
        this.qualityValuePath = qualityValuePath;
        this.qualityAggPath = qualityAggPath;
        this.qualityReturnPath = qualityReturnPath;
    }

    public HurenceDatasourcePluginQueryRequestParam parseRequest(
            JsonObject requestBody) throws IllegalArgumentException {
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
        Float qualityValue = parseQualityValue(requestBody);

        boolean useQuality;
        if (qualityValue != null) {
            builder.withQualityValue(qualityValue);
            useQuality = true;
        } else {
            useQuality = false;
        }
        builder.withUseQuality(useQuality);

        QualityAgg qualityAgg = parseQualityAgg(requestBody);
        if (qualityAgg != null) {
            builder.withQualityAgg(qualityAgg);
        }
        Boolean qualityReturn = parseQualityReturn(requestBody);
        if (qualityReturn != null) {
            builder.withQualityReturn(qualityReturn);
        }
        return builder.build();
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

    private Boolean parseQualityReturn(JsonObject requestBody) {
        return parseBoolean(requestBody, qualityReturnPath);
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

                            String fieldName = jsonObject.getString(FIELD_NAME);

                            JsonObject toReturn = new JsonObject()
                                    .put(FIELD_NAME, fieldName);
                            Map<String, String> tags = parseTags(jsonObject);
                            Float qualityValue = parseQualityValue(jsonObject);
                            if (qualityValue != null)
                                toReturn.put(QUALITY_VALUE, qualityValue);
                            if (!tags.isEmpty())
                                toReturn.put(FIELD_TAGS, tags);
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

    private Float parseQualityValue(JsonObject requestBody) {
        return parseFloat(requestBody, qualityValuePath);
    }

    private QualityAgg parseQualityAgg(JsonObject requestBody) {
        return parseQualityAggregation(requestBody, qualityAggPath);
    }

}
