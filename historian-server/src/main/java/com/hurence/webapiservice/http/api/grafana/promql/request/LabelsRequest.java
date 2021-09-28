package com.hurence.webapiservice.http.api.grafana.promql.request;

import com.hurence.webapiservice.http.api.grafana.promql.parameter.ErrorParameter;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.MatchParameter;
import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hurence.historian.model.HistorianServiceFields.*;

@Data
@Builder
public class LabelsRequest {


    private String name;
    private Long start;
    private Long end;
    private ErrorParameter errors;

    public static class LabelsRequestBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(LabelsRequestBuilder.class);
        private ErrorParameter errors = new ErrorParameter();

        public LabelsRequestBuilder parameters(Map<String,String> parameters) {
            LOGGER.debug("trying to parse requestBody : {}", parameters);

            try {
                start = Long.parseLong(parameters.get(START)) * 1000;
            } catch (Exception exception) {
                errors.addError(exception);
            }

            try {
                end = Long.parseLong(parameters.get(END)) * 1000;
            } catch (Exception exception) {
                errors.addError(exception);
            }

            try {
                name = parameters.get(NAME);
            } catch (Exception exception) {
                errors.addError(exception);
            }

            return this;
        }
    }
}
