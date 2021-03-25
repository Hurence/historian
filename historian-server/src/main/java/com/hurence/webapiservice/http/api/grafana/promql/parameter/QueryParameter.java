package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.RoutingContext;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hurence.historian.model.HistorianServiceFields.*;


/**
 * min(U004_TC01{type="temperature", sub_unit="reacteur1_coquille1", sample="true", bucket="12"})
 */
@Data
@Builder
public class QueryParameter {

    private List<String> keys;
    private String name;
    private Map<String, String> tags;
    private Optional<AggregationOperator> aggregationOperator;

    // NONE, FIRST, AVERAGE, MODE_MEDIAN, LTTB, MIN_MAX, MIN, MAX
    private String sampling;
    private Integer samplingBucketSize;
    private Boolean quality;


    private static Pattern pattern = Pattern.compile("(\\S+)\\{(\\S+)?\\}");

    public static class QueryParameterBuilder {

        private static final Logger LOGGER = LoggerFactory.getLogger(QueryParameterBuilder.class);

        public QueryParameterBuilder parameters(Map<String, String> parameters) {
            if (tags == null)
                tags = new HashMap<>();

            // parsing query
            if(!parameters.containsKey(QUERY))
                throw new IllegalArgumentException(QUERY + " key not found in parameters");

            String queryStr = parameters
                    .get(QUERY)
                    .replaceAll("\\s+", "");
            aggregationOperator = AggregationOperator.findMatching(queryStr);

            // remove operator
            if (aggregationOperator.isPresent())
                queryStr = queryStr
                        .replace(aggregationOperator.get().label, "")
                        .replace("(", "")
                        .replace(")", "");

            Matcher matcher = pattern.matcher(queryStr);

            if (matcher.matches()) {
                name = matcher.group(1);

                if (matcher.group(2) != null) {
                    String[] tagsTupples = matcher.group(2).split(",");
                    for (String t : tagsTupples) {
                        String[] tag = t.split("=");
                        tags.put(tag[0], tag[1].replaceAll("\"", ""));
                    }
                }
            }else{
                // bad trick for strings like "U1230" without "{"
                name = queryStr;
            }

            return this;
        }
    }

}
