package com.hurence.webapiservice.http.api.grafana.promql.request;

import com.hurence.webapiservice.http.api.grafana.promql.parameter.ErrorParameter;
import com.hurence.webapiservice.http.api.grafana.promql.parameter.QueryParameter;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
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

        public QueryRequestBuilder body(String bodyAsString) {
            LOGGER.debug("trying to parse context from by string: {}", bodyAsString);

            if(bodyAsString.isEmpty())
                return this;

            String[] keyValues = bodyAsString.split("&");
            Map<String, String> bodyParameters = new HashMap<>();
            for(String kv : keyValues){
                String[] split = kv.split("=");
                bodyParameters.put(split[0],split[1]);
            }

            return parameters(bodyParameters);
        }

        public QueryRequestBuilder parameters(Map<String, String> parameters) {
            LOGGER.debug("trying to parse context : {}", parameters);

            try{

                // parsing query
                if (!parameters.containsKey(QUERY))
                    throw new IllegalArgumentException(QUERY + " key not found in parameters");

                query = QueryParameter.builder().parse(parameters.get(QUERY)).build();

                if(parameters.containsKey(TIME)){
                    time = Long.parseLong(parameters.get(TIME))*1000;
                    start = time;
                    end = time;
                }

                if(parameters.containsKey(TIMEOUT)){
                    timeout = Long.parseLong(parameters.get(TIMEOUT));
                }

                if(parameters.containsKey(STEP)){
                    step = Integer.parseInt(parameters.get(STEP));
                }

                if(parameters.containsKey(START)){
                    start = Long.parseLong(parameters.get(START))*1000;
                    time = start;
                }

                if(parameters.containsKey(END)){
                    end = Long.valueOf(parameters.get(END))*1000;
                }
            }catch (Exception exception){
                errors.addError(exception);
            }

            return this;
        }
    }
}
