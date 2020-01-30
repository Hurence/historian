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
import com.hurence.logisland.timeseries.functions.math.NElements;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Bottom transformation get the value bottom values
 *
 * @author f.lautenschlager
 */
public final class Bottom implements ChronixTransformation<MetricTimeSeries> {

    private final int value;

    /**
     * Constructs the bottom value values transformation
     *
     * @param args the first parameter is the threshold for the lowest values
     */
    public Bottom(String[] args) {
        this.value = Integer.parseInt(args[0]);
    }

    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {
        NElements.NElementsResult result = NElements.calc(NElements.NElementsCalculation.BOTTOM, value, timeSeries.getTimestampsAsArray(), timeSeries.getValuesAsArray());

        //remove old time series
        timeSeries.clear();
        timeSeries.addAll(result.getNTimes(), result.getNValues());
        functionValueMap.add(this);
    }


    @Override
    public String getQueryName() {
        return "bottom";
    }

    @Override
    public String getTimeSeriesType() {
        return "metric";
    }

    @Override
    public String[] getArguments() {
        return new String[]{"value=" + value};
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
        Bottom rhs = (Bottom) obj;
        return new EqualsBuilder()
                .append(this.value, rhs.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(value)
                .toHashCode();
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value)
                .toString();
    }
}
