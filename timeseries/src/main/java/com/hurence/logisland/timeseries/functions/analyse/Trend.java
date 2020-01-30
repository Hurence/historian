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
import com.hurence.logisland.timeseries.functions.ChronixAnalysis;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import com.hurence.logisland.timeseries.functions.math.LinearRegression;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * The trend analysis
 *
 * @author f.lautenschlager
 */
public final class Trend implements ChronixAnalysis<MetricTimeSeries> {

    /**
     * Detects trends in time series using a linear regression.
     *
     * @param functionValueMap
     * @return 1 if there is a positive trend, otherwise -1
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        //We need to sort the time series for this analysis
        timeSeries.sort();
        //Calculate the linear regression
        LinearRegression linearRegression = new LinearRegression(timeSeries.getTimestamps(), timeSeries.getValues());
        double slope = linearRegression.slope();
        //If we have a positive slope, we return 1 otherwise -1
        functionValueMap.add(this, slope > 0, null);

    }

    @Override
    public String getQueryName() {
        return "trend";
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
