package com.hurence.webapiservice.http.api.grafana.promql.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum TimeserieFunctionType {

    NOOP("noop", "do nothing"),
    ABS("abs", "calculate absolute value over dimensions"),
    SUM("sum", "calculate sum over dimensions"),
    MIN("min", "select minimum over dimensions"),
    MAX("max", "select maximum over dimensions"),
    AVG("avg", "calculate the average over dimensions"),
    STDDEV("stddev", "calculate population standard deviation over dimensions"),
   // STDVAR("stdvar"),
    COUNT("count", "count number of elements in the vector"),
    COUNT_VALUES("count_values", "count number of elements with the same value"),
   // GROUP("group"),
   // BOTTOMK("bottomk", "smallest k elements by sample value"),
   // TOPK("topk", "largest k elements by sample value"),
   // QUANTILE("quantile", "calculate φ-quantile (0 ≤ φ ≤ 1) over dimension"),
    PREDICT_ARIMA("predict_arima", "predict_arima(v range-vector, t scalar) predicts the value of" +
           " time series t seconds from now, based on the range vector v, using ARIMA alogirithm.\n" +
           "predict_linear should only be used with gauges."),
    RATE("rate", "rate(v range-vector) calculates the per-second average rate of increase of " +
            "the time series in the range vector. Breaks in monotonicity " +
            "(such as counter resets due to target restarts) are automatically adjusted for. " +
            "Also, the calculation extrapolates to the ends of the time range, allowing for missed scrapes or " +
            "imperfect alignment of scrape cycles with the range's time period."),
    INCREASE("increase", "increase(v range-vector) calculates the increase in the time series " +
            "in the range vector. Breaks in monotonicity (such as counter resets due to target restarts) are " +
            "automatically adjusted for. The increase is extrapolated to cover the full time range as specified " +
            "in the range vector selector, so that it is possible to get a non-integer result even if a counter " +
            "increases only by integer increments.");

    public final String label;
    public final String desc;

    TimeserieFunctionType(String label, String desc) {
        this.label = label;
        this.desc = desc;
    }
    private static final Map<String, TimeserieFunctionType> BY_LABEL = new HashMap<>();

    static {
        for (TimeserieFunctionType e : values()) {
            BY_LABEL.put(e.label, e);
        }
    }

    public static TimeserieFunctionType valueOfLabel(String label) {
        return BY_LABEL.get(label);
    }

    public static List<String> labels(){
        return Stream.of(values())
                .map( op -> op.label)
                .collect(Collectors.toList());
    }

    public static Optional<TimeserieFunctionType> findMatching(String stringToMatch){
        return labels()
                .stream()
                .filter(stringToMatch::contains)
                .findFirst()
                .map(TimeserieFunctionType::valueOfLabel);
    }

}
