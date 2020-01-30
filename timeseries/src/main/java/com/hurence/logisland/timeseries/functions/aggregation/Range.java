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
package com.hurence.logisland.timeseries.functions.aggregation;

import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.functions.ChronixAggregation;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * The range analysis returns the difference between the maximum and minimum of a time series
 *
 * @author f.lautenschlager
 */
public final class Range implements ChronixAggregation<MetricTimeSeries> {

    /**
     * Gets difference between the maximum and the minimum value.
     * It is always a positive value.
     *
     * @param timeSeries the time series
     * @return the average or 0 if the list is empty
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {
        //If it is empty, we return NaN
        if (timeSeries.size() <= 0) {
            functionValueMap.add(this, Double.NaN);
            return;
        }

        //the values to iterate
        double[] values = timeSeries.getValuesAsArray();
        //Initialize the values with the first element
        double min = values[0];
        double max = values[0];

        for (int i = 1; i < values.length; i++) {
            double current = values[i];

            //check for min
            if (current < min) {
                min = current;
            }
            //check of max
            if (current > max) {
                max = current;
            }
        }
        //return the absolute difference
        functionValueMap.add(this, Math.abs(max - min));
    }

    @Override
    public String getQueryName() {
        return "range";
    }

    @Override
    public String getTimeSeriesType() {
        return "metric";
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
        return new EqualsBuilder()
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .toHashCode();
    }
}
