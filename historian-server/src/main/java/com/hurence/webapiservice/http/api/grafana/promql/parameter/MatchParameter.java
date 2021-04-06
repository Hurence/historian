package com.hurence.webapiservice.http.api.grafana.promql.parameter;



import com.hurence.webapiservice.http.api.grafana.promql.converter.PromQLSynonymLookup;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.hurence.historian.model.HistorianServiceFields.*;


@Data
@Builder
public class MatchParameter {

    private String name;
    private Map<String, String> tags;
    private String luceneQuery;


    public static class MatchParameterBuilder {

        private Map<String, String> tags= new HashMap<>();

        private static final Logger LOGGER = LoggerFactory.getLogger(MatchParameterBuilder.class);

        public MatchParameterBuilder parameters(Map<String, String> parameters) {

                      // parsing query
            if(!parameters.containsKey(MATCH))
                throw new IllegalArgumentException(MATCH + " key not found in parameters");

            String[] matches = parameters.get(MATCH)
                    .replaceAll("\\{", "")
                    .replaceAll("\\}", "")
                    .split(",");

            StringBuilder luceneQueryBuilder = new StringBuilder();
            for (String m : matches) {
                String[] arg = m.split("=");
                String key = arg[0];
                String value = arg[1].replaceAll("\"", "");
                if(value.contains(" "))
                    value = String.format("\"%s\"", value);

                if(key.equals(__NAME__)){
                    name = PromQLSynonymLookup.getOriginalName(value);
                    if (luceneQueryBuilder.length() != 0)
                        luceneQueryBuilder.append(" AND");

                    luceneQueryBuilder.append(String.format(" name:%s", name));
                }else {
                    tags.put(key, value);
                    if (luceneQueryBuilder.length() != 0)
                        luceneQueryBuilder.append(" AND");
                    luceneQueryBuilder.append(String.format(" %s:%s", key, value));
                }
            }
            luceneQuery = luceneQueryBuilder.toString().trim();

            return this;
        }
    }

}
