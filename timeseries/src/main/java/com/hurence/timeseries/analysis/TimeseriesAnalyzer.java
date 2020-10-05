package com.hurence.timeseries.analysis;


import lombok.Builder;
import lombok.Value;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.List;

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
        values.forEach(stats::addValue);

        if (computeStats) {
            builder.mean(stats.getMean())
                    .first(values.size() == 0 ? Double.NaN : values.get(0))
                    .last(values.size() == 0 ? Double.NaN : values.get(values.size() - 1))
                    .min(stats.getMin())
                    .max(stats.getMax())
                    .stdDev(stats.getStandardDeviation())
                    .sum(stats.getSum())
                    .kurtosis(stats.getKurtosis())
                    .skewness(stats.getSkewness());
        }

        if (computeTrend) {
            SimpleRegression regression = new SimpleRegression();
            for (int i = 0; i < timestamps.size(); i++) {
                regression.addData(timestamps.get(0), values.get(0));
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
            for (double point : values) {
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
