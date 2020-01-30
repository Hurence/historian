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

/**
 * The add transformation
 *
 * @author f.lautenschlager
 */
public final class Add implements ChronixTransformation<MetricTimeSeries> {

    private final double value;

    /**
     * Constructs the add transformation
     *
     * @param args the first parameter is double that is added to evey value.
     */
    public Add(String[] args) {
        this.value = Double.parseDouble(args[0]);
    }

    /**
     * Adds the increment to each value of the time series.
     * <pre>
     * foreach(value){
     *     value += increment;
     * }
     * </pre>
     *
     * @param functionValueMap to add the this transformation to.
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {

        if (timeSeries.isEmpty()) {
            return;
        }

        long[] timestamps = timeSeries.getTimestampsAsArray();
        double[] values = timeSeries.getValuesAsArray();

        timeSeries.clear();

        for (int i = 0; i < values.length; i++) {
            values[i] += value;
        }

        timeSeries.addAll(timestamps, values);

        functionValueMap.add(this);
    }

    @Override
    public String getQueryName() {
        return "add";
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
        Add rhs = (Add) obj;
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
