package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum AggregationOperator {

    SUM("sum"),
    MIN("min"),
    MX("max"),
    AVG("avg"),
    STDDEV("stddev"),
    STDVAR("stdvar"),
    COUNT("count"),
    COUNT_VALUES("count_values"),
    GROUP("group"),
    BOTTOMK("bottomk"),
    TOPK("topk"),
    QUANTILE("quantile"),
    PREDICT_ARIMA("predict_arima"),
    RATE("rate"),
    INCREASE("increase");

    public final String label;

    private AggregationOperator(String label) {
        this.label = label;
    }
    private static final Map<String, AggregationOperator> BY_LABEL = new HashMap<>();

    static {
        for (AggregationOperator e : values()) {
            BY_LABEL.put(e.label, e);
        }
    }

    public static AggregationOperator valueOfLabel(String label) {
        return BY_LABEL.get(label);
    }

    public static List<String> labels(){
        return Stream.of(values())
                .map( op -> op.label)
                .collect(Collectors.toList());
    }

    public static Optional<AggregationOperator> findMatching(String stringToMatch){
        return labels()
                .stream()
                .filter(stringToMatch::contains)
                .findFirst()
                .map(AggregationOperator::valueOfLabel);
    }

}
