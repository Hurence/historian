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
import com.hurence.logisland.timeseries.converter.common.DoubleList;
import com.hurence.logisland.timeseries.converter.common.LongList;
import com.hurence.logisland.timeseries.functions.ChronixTransformation;
import com.hurence.logisland.timeseries.functions.FunctionValueMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * The distinct transformation.
 *
 * @author f.lautenschlager
 */
public final class Distinct implements ChronixTransformation<MetricTimeSeries> {
    /**
     * Transforms a time series into a representation with distinct values.
     * The distinct operation uses the first occurrence of a point.
     *
     * @param timeSeries        the time series that is transformed
     * @param functionValueMap the function value map
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        if (timeSeries.isEmpty()) {
            return;
        }

        timeSeries.sort();

        LongList timeList = new LongList(timeSeries.size());
        DoubleList valueList = new DoubleList(timeSeries.size());

        //We should use a other data structure...
        Set<Double> distinct = new HashSet<>();

        for (int i = 0; i < timeSeries.size(); i++) {
            double value = timeSeries.getValue(i);

            if (!distinct.contains(value)) {
                timeList.add(timeSeries.getTime(i));
                valueList.add(value);
                distinct.add(value);
            }
        }
        timeSeries.clear();
        timeSeries.addAll(timeList, valueList);

        functionValueMap.add(this);
    }

    @Override
    public String getQueryName() {
        return "distinct";
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
