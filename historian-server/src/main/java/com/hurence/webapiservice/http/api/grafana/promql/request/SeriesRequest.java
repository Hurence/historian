package com.hurence.webapiservice.http.api.grafana.promql.request;

import com.hurence.webapiservice.http.api.grafana.promql.parameter.ErrorParameter;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.MatchParameter;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.hurence.historian.model.HistorianServiceFields.*;

@Data
@Builder
public class SeriesRequest {
    private Map<String, String> match;
    private String luceneQuery;
    private Long start;
    private Long end;
    private MatchParameter matchParameter;
    private ErrorParameter errors;

    public static class SeriesRequestBuilder {
        private ErrorParameter errors = new ErrorParameter();
        private static final Logger LOGGER = LoggerFactory.getLogger(SeriesRequestBuilder.class);

        public SeriesRequestBuilder parameters(Map<String, String> parameters) {
            LOGGER.debug("trying to parse requestBody : {}", parameters);

            try {
                start = Long.parseLong(parameters.get(START));
            } catch (Exception exception) {
                errors.addError(exception);
            }

            try {
                end = Long.parseLong(parameters.get(END));
            } catch (Exception exception) {
                errors.addError(exception);
            }

            try{
                matchParameter = MatchParameter.builder().parameters(parameters).build();
            }catch (Exception exception) {
                errors.addError(exception);
            }


            return this;
        }
    }
}
