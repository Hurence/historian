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
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * Divide transformation
 *
 * @author f.lautenschlager
 */
public final class Divide implements ChronixTransformation<MetricTimeSeries> {

    private final double value;

    /**
     * Scales the time series by the given value
     *
     * @param args the first value is the divisor
     */
    public Divide(String[] args) {
        this.value = Double.parseDouble(args[0]);
    }

    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        //Get a copy of the values
        double[] values = timeSeries.getValuesAsArray();
        //Get a copy of the timestamps
        long[] times = timeSeries.getTimestampsAsArray();
        for (int i = 0; i < timeSeries.size(); i++) {
            //simply divide the original value
            values[i] = values[i] / value;
        }
        //Clear the original time series and add the values
        timeSeries.clear();
        timeSeries.addAll(times, values);

        functionValueMap.add(this);
    }

    @Override
    public String getQueryName() {
        return "divide";
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
        Divide rhs = (Divide) obj;
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


}
