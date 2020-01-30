/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.timeseries.functions.analyse;


import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.MultivariateTimeSeries;
import com.hurence.logisland.timeseries.distance.DistanceFunction;
import com.hurence.logisland.timeseries.distance.DistanceFunctionEnum;
import com.hurence.logisland.timeseries.distance.DistanceFunctionFactory;
import com.hurence.logisland.timeseries.dts.Pair;
import com.hurence.logisland.timeseries.dtw.FastDTW;
import com.hurence.logisland.timeseries.dtw.TimeWarpInfo;
import com.hurence.logisland.timeseries.functions.ChronixPairAnalysis;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * The analysis implementation of the Fast DTW analysis
 *
 * @author f.lautenschlager
 */
public final class FastDtw implements ChronixPairAnalysis<Pair<MetricTimeSeries, MetricTimeSeries>> {

    private final DistanceFunction distanceFunction;
    private final int searchRadius;
    private final double maxNormalizedWarpingCost;
    private final String subquery;

    /**
     * @param args The first argument is the subquery, the second is the search radius, the third is the maxNormalizedWarpingCost
     */
    public FastDtw(String[] args) {

        this.subquery = removeBrackets(args[0]);
        this.searchRadius = Integer.parseInt(args[1]);
        this.maxNormalizedWarpingCost = Double.parseDouble(args[2]);
        this.distanceFunction = DistanceFunctionFactory.getDistanceFunction(DistanceFunctionEnum.EUCLIDEAN);
    }

    private static String removeBrackets(String subQuery) {
        //remove the enfolding brackets
        if (subQuery.indexOf('(') == 0 && subQuery.lastIndexOf(')') == subQuery.length() - 1) {
            return subQuery.substring(1, subQuery.length() - 1);
        }
        return subQuery;
    }

    @Override
    public void execute(Pair<MetricTimeSeries, MetricTimeSeries> timeSeriesPair, FunctionValueMap functionValueMap) {
        //We have to build a multivariate time series
        MultivariateTimeSeries origin = buildMultiVariateTimeSeries(timeSeriesPair.getFirst());
        MultivariateTimeSeries other = buildMultiVariateTimeSeries(timeSeriesPair.getSecond());
        //Call the fast dtw library
        TimeWarpInfo result = FastDTW.getWarpInfoBetween(origin, other, searchRadius, distanceFunction);
        //Check the result. If it lower equals the threshold, we can return the other time series
        functionValueMap.add(this, result.getNormalizedDistance() <= maxNormalizedWarpingCost, timeSeriesPair.getSecond().getName());

    }

    /**
     * Builds a multivariate time series of the given univariate time series.
     * If two or more timestamps are the same, the values are aggregated using the average.
     *
     * @param timeSeries the metric time series
     * @return a multivariate time series for the fast dtw analysis
     */
    private MultivariateTimeSeries buildMultiVariateTimeSeries(MetricTimeSeries timeSeries) {
        MultivariateTimeSeries multivariateTimeSeries = new MultivariateTimeSeries(1);

        if (timeSeries.size() > 0) {
            //First sort the values
            timeSeries.sort();

            long formerTimestamp = timeSeries.getTime(0);
            double formerValue = timeSeries.getValue(0);
            int timesSameTimestamp = 0;

            for (int i = 1; i < timeSeries.size(); i++) {

                //We have two timestamps that are the same
                if (formerTimestamp == timeSeries.getTime(i)) {
                    formerValue += timeSeries.getValue(i);
                    timesSameTimestamp++;
                } else {
                    //calc the average of the values of the same timestamp
                    if (timesSameTimestamp > 0) {
                        formerValue = formerValue / timesSameTimestamp;
                        timesSameTimestamp = 0;
                    }
                    //first add the former timestamp
                    multivariateTimeSeries.add(formerTimestamp, new double[]{formerValue});
                    formerTimestamp = timeSeries.getTime(i);
                    formerValue = timeSeries.getValue(i);
                }
            }
            //add the last point
            multivariateTimeSeries.add(formerTimestamp, new double[]{formerValue});
        }

        return multivariateTimeSeries;
    }

    @Override
    public String[] getArguments() {
        return new String[]{"search radius=" + searchRadius,
                "max warping cost=" + maxNormalizedWarpingCost,
                "distance function=" + DistanceFunctionEnum.EUCLIDEAN.name()};
    }

    @Override
    public String getQueryName() {
        return "fastdtw";
    }


    @Override
    public String getTimeSeriesType() {
        return "metric";
    }

    @Override
    public boolean needSubquery() {
        return true;
    }

    @Override
    public String getSubquery() {
        return subquery;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        FastDtw rhs = (FastDtw) obj;
        return new EqualsBuilder()
                .append(this.distanceFunction, rhs.distanceFunction)
                .append(this.searchRadius, rhs.searchRadius)
                .append(this.maxNormalizedWarpingCost, rhs.maxNormalizedWarpingCost)
                .append(this.subquery, rhs.subquery)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(distanceFunction)
                .append(searchRadius)
                .append(maxNormalizedWarpingCost)
                .append(subquery)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "FastDtw{" +
                "distanceFunction=" + distanceFunction +
                ", searchRadius=" + searchRadius +
                ", maxNormalizedWarpingCost=" + maxNormalizedWarpingCost +
                ", subquery='" + subquery + '\'' +
                '}';
    }
}
