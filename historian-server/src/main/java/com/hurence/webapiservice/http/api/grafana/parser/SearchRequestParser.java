package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.webapiservice.http.api.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.SearchRequestParam;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchRequestParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchRequestParser.class);


    public SearchRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse requestBody : {}", requestBody);
        SearchRequestParam.Builder builder = new SearchRequestParam.Builder();
        String stringToUseToFindMetrics = parseMetricName(requestBody);
        builder.withStringToUSeToFindMetrics(stringToUseToFindMetrics);
        Integer maxNumberOfMetricNameToReturn = parseMaxNumberOfMetricToReturn(requestBody);;
        builder.withMaxNumberOfMetricToReturn(maxNumberOfMetricNameToReturn == null ? QueryRequestParam.DEFAULT_MAX_DATAPOINTS : maxNumberOfMetricNameToReturn);
        return builder.build();
    }

    private String parseMetricName(JsonObject requestBody) {
        return requestBody.getString("target");
    }

    private Integer parseMaxNumberOfMetricToReturn(JsonObject requestBody) {
        return requestBody.getInteger("limit");
    }
}
