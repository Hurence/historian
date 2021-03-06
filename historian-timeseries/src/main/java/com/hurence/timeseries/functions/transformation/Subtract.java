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
package com.hurence.timeseries.functions.transformation;

import com.hurence.timeseries.MetricTimeSeries;
import com.hurence.timeseries.functions.ChronixTransformation;
import com.hurence.timeseries.functions.FunctionValueMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * The subtract transformation
 *
 * @author f.lautenschlager
 */
public final class Subtract implements ChronixTransformation<MetricTimeSeries> {

    private final double value;

    /**
     * Constructs the subtract transformation.
     * The value is subtracted from each time series value
     *
     * @param args the first value is the amount that is subtracted
     */
    public Subtract(String[] args) {
        this.value = Double.parseDouble(args[0]);
    }

    /**
     * Subtracts the value from each time series value
     *
     * @param timeSeries the time series that is transformed
     * @return the transformed time series
     */
    @Override
    public void execute(MetricTimeSeries timeSeries, FunctionValueMap functionValueMap) {
        long[] timestamps = timeSeries.getTimestampsAsArray();
        double[] values = timeSeries.getValuesAsArray();

        timeSeries.clear();

        for (int i = 0; i < values.length; i++) {
            values[i] -= value;
        }

        timeSeries.addAll(timestamps, values);
        functionValueMap.add(this);
    }

    @Override
    public String getQueryName() {
        return "sub";
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
    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value)
                .toString();
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
        Subtract rhs = (Subtract) obj;
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
