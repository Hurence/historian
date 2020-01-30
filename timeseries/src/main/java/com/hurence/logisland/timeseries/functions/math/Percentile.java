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
package com.hurence.logisland.timeseries.functions.math;


import com.hurence.logisland.timeseries.converter.common.DoubleList;

import java.util.Arrays;

/**
 * Class to calculate a percentile
 *
 * @author f.lautenschlager
 */
public final class Percentile {

    /**
     * Avoid instances
     */
    private Percentile() {
    }

    /**
     * Implemented the quantile type 7 referred to
     * http://tolstoy.newcastle.edu.au/R/e17/help/att-1067/Quartiles_in_R.pdf
     * and
     * http://stat.ethz.ch/R-manual/R-patched/library/stats/html/quantile.html
     * as its the default quantile implementation
     * <p>
     * <code>
     * QuantileType7 = function (v, p) {
     * v = sort(v)
     * h = ((length(v)-1)*p)+1
     * v[floor(h)]+((h-floor(h))*(v[floor(h)+1]- v[floor(h)]))
     * }
     * </code>
     *
     * @param values     - the values to aggregate the percentile
     * @param percentile - the percentile (0 - 1), e.g. 0.25
     * @return the value of the n-th percentile
     */
    public static double evaluate(DoubleList values, double percentile) {
        double[] doubles = values.toArray();
        Arrays.sort(doubles);

        return evaluateForDoubles(doubles, percentile);
    }

    private static double evaluateForDoubles(double[] points, double percentile) {
        //For example:
        //values    = [1,2,2,3,3,3,4,5,6], size = 9, percentile (e.g. 0.25)
        // size - 1 = 8 * 0.25 = 2 (~ 25% from 9) + 1 = 3 => values[3] => 2
        double percentileIndex = ((points.length - 1) * percentile) + 1;

        double rawMedian = points[floor(percentileIndex - 1)];
        double weight = percentileIndex - floor(percentileIndex);

        if (weight > 0) {
            double pointDistance = points[floor(percentileIndex - 1) + 1] - points[floor(percentileIndex - 1)];
            return rawMedian + weight * pointDistance;
        } else {
            return rawMedian;
        }
    }

    /**
     * Wraps the Math.floor function and casts it to an integer
     *
     * @param value - the evaluatedValue
     * @return the floored evaluatedValue
     */
    private static int floor(double value) {
        return (int) Math.floor(value);
    }


}
