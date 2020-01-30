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
package com.hurence.logisland.timeseries.functions.transformation;

import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.functions.ChronixTransformation;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Arrays;

/**
 * This transformation does a vectorization of the time series by removing some points.
 *
 * @author f.lautenschlager
 */
public final class Vectorization implements ChronixTransformation<MetricTimeSeries> {

    private final float tolerance;

    /**
     * Constructs the vectorization transformation.
     * <p>
     * A typical tolerance value is 0.01f
     *
     * @param args the first value is used to decide if the distance of values is almost equals.
     */
    public Vectorization(String[] args) {
        this.tolerance = Float.parseFloat(args[0]);
    }


    /**
     * Todo: Describe the algorithm, a bit.
     * <p>
     * Note: The transformation changes the values of the time series!
     * Further analyses such as aggregations uses the transformed values for the calculation.
     *
     * @param timeSeries the time series that is transformed
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        // we need a sorted time series
        timeSeries.sort();

        int size = timeSeries.size();
        // do not simplify if there are insufficient data points
        if (size <= 3) {
            return;
        }

        byte[] usePoint = new byte[size];
        Arrays.fill(usePoint, (byte) 1);

        long[] rawTimeStamps = timeSeries.getTimestampsAsArray();
        double[] rawValues = timeSeries.getValuesAsArray();

        // Clear the original time series
        timeSeries.clear();

        compute(rawTimeStamps, rawValues, usePoint, tolerance);

        for (int i = 0; i < size; i++) {
            if (usePoint[i] == 1) {
                timeSeries.add(rawTimeStamps[i], rawValues[i]);
            }
        }
        functionValueMap.add(this);
    }

    /**
     * Calculates the distance between a point and a line.
     * The distance function is defined as:
     * <p>
     * <code>
     * (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)</p>
     * s = -----------------------------</p>
     * L^2</p>
     * </code>
     * Then the distance from C to P = |s|*L.
     */
    private double getDistance(long pX, double pY, long aX, double aY, long bX, double bY) {

        double l2 = (bX - aX) * (bX - aX) + (bY - aY) * (bY - aY);
        double s = ((aY - pY) * (bX - aX) - (aX - pX) * (bY - aY)) / (l2);

        return Math.abs(s) * Math.sqrt(l2);
    }

    private void compute(long[] timestamps, double[] values, byte[] usePoint, float tolerance) {

        int ixA = 0;
        int ixB = 1;
        for (int i = 2; i < timestamps.length; i++) {
            double dist = getDistance(timestamps[i], values[i], timestamps[ixA], values[ixA], timestamps[ixB], values[ixB]);

            if (dist < tolerance) {
                usePoint[i - 1] = 0;
                continue;
            }
            //reached the end
            if (i + 1 == timestamps.length) {
                return;
            }
            // continue with next point
            ixA = i;
            ixB = i + 1;
            i++;
        }
    }

    @Override
    public String getQueryName() {
        return "vector";
    }

    @Override
    public String getTimeSeriesType() {
        return "metric";
    }

    @Override
    public String[] getArguments() {
        return new String[]{"tolerance=" + tolerance};
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
        Vectorization rhs = (Vectorization) obj;
        return new EqualsBuilder()
                .append(this.tolerance, rhs.tolerance)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(tolerance)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tolerance", tolerance)
                .toString();
    }
}
