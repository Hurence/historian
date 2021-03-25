package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.hurence.webapiservice.http.api.grafana.promql.request.QueryRequest;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Data
public class ErrorParameter {

    protected String error;
    protected String errorType;
    protected List<String> warnings = new ArrayList<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorParameter.class);
    public void addError(Exception exception) {

        LOGGER.debug(exception.getMessage());

        errorType = "query_parsing_error";
        error = exception.getMessage();

    }
}
