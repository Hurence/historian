package com.hurence.timeseries.analysis;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class TimeseriesAnalysis implements Serializable {

    long count;

    /**
     * Detects trends in time series using a linear regression.
     * @return true if there is a positive trend, false otherwise
     */
    boolean hasTrend;

    /**
     * Detects outliers using the default box plot implementation.
     * An outlier every value that is above (q3-q1)*1.5*q3 where qN is the nth percentile
     * @return true if there were an outlier, false otherwise
     */
    boolean hasOutlier;

    /**
     * Returns the minimum of the available values
     * @return The min or Double.NaN if no values have been added.
     */
    double min;

    /**
     * Returns the maximum of the available values
     * @return The max or Double.NaN if no values have been added.
     */
    double max;

    /**
     * Returns the <a href="http://www.xycoon.com/arithmetic_mean.htm">
     * arithmetic mean </a> of the available values
     * @return The mean or Double.NaN if no values have been added.
     */
    double mean;


    /**
     * Returns the sum of the values that have been added to Univariate.
     * @return The sum or Double.NaN if no values have been added
     */
    double sum;


    /**
     * Returns the standard deviation of the available values.
     * @return The standard deviation, Double.NaN if no values have been added
     * or 0.0 for a single value set.
     */
    double stdDev;

    /**
     * Returns the <a href="http://en.wikibooks.org/wiki/Statistics/Summary/Variance">
     * population variance</a> of the available values.
     *
     * @return The population variance, Double.NaN if no values have been added,
     * or 0.0 for a single value set.
     */
    double variance;

    /**
     * Returns the first element of the available values.
     *
     * @return The first element, Double.NaN if no values have been added
     */
    double first;

    /**
     * Returns the last element of the available values.
     *
     * @return The last element, Double.NaN if no values have been added
     */
    double last;
    /**
     * Returns the skewness of the available values. Skewness is a
     * measure of the asymmetry of a given distribution.
     *
     * @return The skewness, Double.NaN if less than 3 values have been added.
     */
    double skewness;

    /**
     * Returns the Kurtosis of the available values. Kurtosis is a
     * measure of the "peakedness" of a distribution.
     *
     * @return The kurtosis, Double.NaN if less than 4 values have been added.
     */
    double kurtosis;
}
