package com.hurence.webapiservice.http.api.grafana.promql.parameter;


import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
@Builder
public class RangeDuration {

    private Duration duration;
    private String queryWithoutDuration;

    public static class RangeDurationBuilder {

        private String durationString;

        private static final String regex = ".*\\[(\\S+)\\].*";

        public RangeDurationBuilder parse(String query) {

            final Pattern pattern = Pattern.compile(regex);
            final Matcher matcher = pattern.matcher(query);

            if (matcher.matches()) {
                String matched = matcher.group(1);
                durationString = matched.toLowerCase(Locale.ROOT).contains("d") ?
                        String.format("P%s",matched) :
                        String.format("PT%s",matched) ;
                this.duration = Duration.parse(durationString);

                this.queryWithoutDuration = query.replaceAll(String.format("\\[%s\\]", matched), "");
            }else {
                duration = Duration.ZERO;
                queryWithoutDuration = query;
            }



            return this;
        }


    }
}
