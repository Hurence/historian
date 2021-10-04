package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.promql.function.TimeserieFunctionType;
import com.hurence.webapiservice.modele.SamplingConf;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hurence.historian.model.HistorianServiceFields.*;


/**
 * min(U004_TC01{type="temperature", sub_unit="reacteur1_coquille1", sample="true", bucket="12"})
 */
@Data
@Builder
public class QueryParameter {

    private String name;
    private Map<String, String> tags;
    private Optional<TimeserieFunctionType> aggregationOperator;

    // NONE, FIRST, AVERAGE, MODE_MEDIAN, LTTB, MIN_MAX, MIN, MAX
    private SamplingConf sampling;
    private Boolean quality;

    private RangeDuration rangeDuration;


    public String toQueryString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);

        if(!tags.isEmpty()){
            sb.append("{");
            tags.forEach( (key, value) -> sb.append(key).append("=").append(value).append(", "));
            sb.delete(sb.length()-2,sb.length());
            sb.append("}");
        }

        return sb.toString();
    }


    public static class QueryParameterBuilder {
        private Map<String, String> tags = new TreeMap<>();
        private SamplingConf sampling = new SamplingConf(SamplingAlgorithm.NONE, 1, 1440);
        private Optional<TimeserieFunctionType> aggregationOperator = Optional.empty();

        private static final Logger LOGGER = LoggerFactory.getLogger(QueryParameterBuilder.class);
        private static Pattern pattern = Pattern.compile("(\\S+)\\{(\\S+)?\\}");

        public QueryParameterBuilder parse(String queryStr) {

            // first get and remove duration
            rangeDuration = RangeDuration.builder().parse(queryStr).build();
            queryStr = rangeDuration.getQueryWithoutDuration();

            queryStr = queryStr
                    .replaceAll("\\{\\s+", "{")
                    .replaceAll("\\s+\\}", "}")
                    .replaceAll(",\\s+", ",")
                    .replaceAll("\\s+", ",");
            aggregationOperator = TimeserieFunctionType.findMatching(queryStr);

            // remove operator
            if (aggregationOperator.isPresent())
                queryStr = queryStr
                        .replace(aggregationOperator.get().label + "(", "")
                        .replace("(", "")
                        .replace(")", "");

            Matcher matcher = pattern.matcher(queryStr);


            if (matcher.matches()) {
                name = matcher.group(1);

                if (matcher.group(2) != null) {
                    String[] tagsTupples = matcher.group(2).split(",");
                    for (String t : tagsTupples) {
                        String[] tag = t.split("=");

                        String tagName = tag[0];
                        String tagValue = tag[1].replaceAll("\"", "").replaceAll("~.", "");
                        if (tagName.equalsIgnoreCase(SAMPLING_ALGO)) {
                            try {
                                sampling.setAlgo(SamplingAlgorithm.valueOf(tagValue.toUpperCase()));
                            } catch (Exception ex) {
                                LOGGER.warn("unable to parse sampling algo : " + tagValue);
                            }
                        } else if (tagName.equalsIgnoreCase(BUCKET_SIZE)) {
                            try {
                                sampling.setBucketSize(Integer.parseInt(tagValue));
                            } catch (Exception ex) {
                                LOGGER.warn("unable to parse bucket size : " + tagValue);
                            }
                        } else if(!tagValue.equalsIgnoreCase("*")){
                            tags.put(tagName, tagValue);
                        }
                    }
                }
            } else {
                // bad trick for strings like "U1230" without "{"
                name = queryStr;
            }

            return this;
        }
    }

}
