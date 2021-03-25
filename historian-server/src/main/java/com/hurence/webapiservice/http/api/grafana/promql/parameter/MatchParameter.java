package com.hurence.webapiservice.http.api.grafana.promql.parameter;



import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.hurence.historian.model.HistorianServiceFields.MATCH;


@Data
@Builder
public class MatchParameter {

    private String matchStr;
    private Map<String, String> match;
    private String luceneQuery;


    public static class MatchParameterBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(MatchParameterBuilder.class);

        public MatchParameterBuilder parameters(Map<String, String> parameters) {

            if (match == null)
                match = new HashMap<>();

            // parsing query
            if(!parameters.containsKey(MATCH))
                throw new IllegalArgumentException(MATCH + " key not found in parameters");

            matchStr = parameters.get(MATCH);

            String[] matches = matchStr
                    .replaceAll("\\{", "")
                    .replaceAll("\\}", "")
                    .split(",");

            StringBuilder luceneQueryBuilder = new StringBuilder();
            for (String m : matches) {
                String[] arg = m.split("=");
                String key = arg[0];
                String value = arg[1].replaceAll("\"", "");
                match.put(key, value);
                if(luceneQueryBuilder.length()!=0)
                    luceneQueryBuilder.append( " AND" );
                luceneQueryBuilder.append( String.format(" %s:\"%s\"", key, value) );
            }
            luceneQuery = luceneQueryBuilder.toString().trim();

            return this;
        }
    }

}
