package com.hurence.timeseries.analysis;


import lombok.Builder;
import lombok.Value;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.List;
import java.util.stream.Collectors;

@Value
@Builder
/**
 * Compute some Timeseries stats
 */
public class TimeseriesAnalyzer {

    @Builder.Default
    boolean computeStats = true;
    @Builder.Default
    boolean computeTrend = true;
    @Builder.Default
    boolean computeOutlier = true;

    public TimeseriesAnalysis run(List<Long> timestamps, List<Double> values) {

        TimeseriesAnalysis.TimeseriesAnalysisBuilder builder = TimeseriesAnalysis.builder();
        DescriptiveStatistics stats = new DescriptiveStatistics();
        List<Double> nonNanValues = values.stream().filter(v -> !v.isNaN()).collect(Collectors.toList());
        nonNanValues.forEach(stats::addValue);

        if (computeStats) {
            builder.mean(stats.getMean())
                    .first(values.size() == 0 ? Double.NaN : values.get(0))
                    .last(values.size() == 0 ? Double.NaN : values.get(values.size() - 1))
                    .min(stats.getMin())
                    .max(stats.getMax())
                    .variance(stats.getVariance())
                    .stdDev(stats.getStandardDeviation())
                    .sum(stats.getSum())
                    .kurtosis(stats.getKurtosis())
                    .skewness(stats.getSkewness());
        }

        if (computeTrend) {
            SimpleRegression regression = new SimpleRegression();
            for (int i = 0; i < timestamps.size(); i++) {

                if (!values.get(i).isNaN())
                    regression.addData(timestamps.get(i), values.get(i));
            }
            builder.hasTrend(regression.getSlope() > 0);
        }

        if (computeOutlier) {
            // defaults to false
            builder.hasOutlier(false);
            // Calculate the percentiles
            double q1 = stats.getPercentile(25);
            double q3 = stats.getPercentile(75);
            // Calculate the threshold
            double threshold = (q3 - q1) * 1.5 + q3;
            // filter the values, if one outlier is found, we can return
            for (double point : nonNanValues) {
                if (point > threshold) {
                    builder.hasOutlier(true);
                    break;
                }
            }
        }

        builder.count(values.size());
        return builder.build();
    }

}
