package com.hurence.webapiservice.http.api.grafana.promql.request;

import com.hurence.webapiservice.http.api.grafana.promql.parameter.ErrorParameter;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.QueryParameter;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;

import static com.hurence.historian.model.HistorianServiceFields.*;

/**
 * can be used for both /query and /query_range endpoints
 */
@Data
@Builder
public class QueryRequest {

    private Long time;
    private Long timeout;
    private QueryParameter query;
    private Long start;
    private Long end;
    private Integer step;

    private ErrorParameter errors;

    public static class QueryRequestBuilder {
        private ErrorParameter errors = new ErrorParameter();
        private static final Logger LOGGER = LoggerFactory.getLogger(QueryRequestBuilder.class);

        public QueryRequestBuilder parameters(Map<String, String> parameters) {
            LOGGER.debug("trying to parse context : {}", parameters);

            try{
                query = QueryParameter.builder().parameters(parameters).build();

                if(parameters.containsKey(TIME)){
                    time = Long.valueOf(parameters.get(TIME));
                    start = time;
                    end = time;
                }
                if(parameters.containsKey(TIMEOUT)){
                    timeout = Long.valueOf(parameters.get(TIMEOUT));
                }
                if(parameters.containsKey(STEP)){
                    step = Integer.valueOf(parameters.get(STEP));
                }

                if(parameters.containsKey(START)){
                    start = Long.valueOf(parameters.get(START));
                    time = start;
                }
                if(parameters.containsKey(END)){
                    end = Long.valueOf(parameters.get(END));
                }
            }catch (Exception exception){
                errors.addError(exception);
            }

            return this;
        }
    }
}
