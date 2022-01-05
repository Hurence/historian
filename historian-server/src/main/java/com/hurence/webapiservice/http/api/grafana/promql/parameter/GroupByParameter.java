package com.hurence.webapiservice.http.api.grafana.promql.parameter;


import com.hurence.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.http.api.grafana.promql.function.TimeserieFunctionType;
import com.hurence.webapiservice.modele.SamplingConf;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Data
@Builder
public class GroupByParameter {


    private List<String> names;
    private String queryWithoutBy;

    public static class GroupByParameterBuilder {
        private List<String> names = new ArrayList<>();


        private static final Logger LOGGER = LoggerFactory.getLogger(GroupByParameter.GroupByParameterBuilder.class);
        private static Pattern pattern = Pattern.compile("(.*)by\\s?\\((\\S+)\\)(.*)");

        public GroupByParameter.GroupByParameterBuilder parse(String queryStr) {


            String queryStrCleanup1 = queryStr.replaceAll(",\\s", ",");


            final Matcher matcher = pattern.matcher(queryStrCleanup1);


            if (matcher.matches()) {
                String matched = matcher.group(2);

                names = Arrays.asList(matched.split(","));

                queryWithoutBy = queryStrCleanup1.replaceAll(String.format("by\\s?\\(%s\\)", matched), "")
                        .replaceAll("\\s", "")
                        .replaceAll(",", ", ");
            }else {
                  queryWithoutBy = queryStr;
            }



            return this;
        }
    }
}
