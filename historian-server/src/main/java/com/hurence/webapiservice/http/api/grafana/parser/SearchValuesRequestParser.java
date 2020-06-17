package com.hurence.webapiservice.http.api.grafana.parser;

import com.hurence.webapiservice.http.api.grafana.modele.QueryRequestParam;
import com.hurence.webapiservice.http.api.grafana.modele.SearchValuesRequestParam;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchValuesRequestParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchRequestParser.class);

    private final String field;
    private final String query;
    private final String limitField;

    public SearchValuesRequestParser(String metricNameField,
                               String query,
                               String limitField) {
        this.field = metricNameField;
        this.query = query;
        this.limitField = limitField;
    }

    public SearchValuesRequestParam parseRequest(JsonObject requestBody) throws IllegalArgumentException {
        LOGGER.debug("trying to parse requestBody : {}", requestBody);
        SearchValuesRequestParam.Builder builder = new SearchValuesRequestParam.Builder();
        String fieldToSearch = parseField(requestBody);
        String queryToUseInSearch = parseQuery(requestBody);
        builder.withField(fieldToSearch);
        builder.withQuery(queryToUseInSearch);
        Integer maxNumberOfMetricNameToReturn = parseMaxNumberOfMetricToReturn(requestBody);;
        builder.withMaxNumberOfMetricToReturn(maxNumberOfMetricNameToReturn == null ? QueryRequestParam.DEFAULT_MAX_DATAPOINTS : maxNumberOfMetricNameToReturn);
        return builder.build();
    }

    private String parseField(JsonObject requestBody) {
        return requestBody.getString(field);
    }

    private String parseQuery(JsonObject requestBody) {
        return requestBody.getString(query);
    }

    private Integer parseMaxNumberOfMetricToReturn(JsonObject requestBody) {
        return requestBody.getInteger(limitField);
    }
}
