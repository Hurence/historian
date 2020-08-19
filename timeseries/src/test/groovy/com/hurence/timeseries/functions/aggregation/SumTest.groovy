/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.timeseries.functions.aggregation

import com.hurence.timeseries.MetricTimeSeries
import com.hurence.timeseries.functions.FunctionValueMap
import com.hurence.timeseries.functions.aggregation.Sum
import spock.lang.Specification

/**
 * Unit test for the sum aggregation
 * @author f.lautenschlager
 */
class SumTest extends Specification {

    def "test execute"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Sum")
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()
        def analysisResult = new FunctionValueMap(1, 0, 0);

        when:
        new Sum().execute(ts, analysisResult)
        then:
        analysisResult.getAggregationValue(0) == 10449.0d
    }

    def "test 2 execute"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Sum")
        timeSeries.point(11, 1)
        timeSeries.point(10, 2)
        MetricTimeSeries ts = timeSeries.build()
        def analysisResult = new FunctionValueMap(1, 0, 0);

        when:
        new Sum().execute(ts, analysisResult)
        then:
        analysisResult.getAggregationValue(0) == 3.0d
    }


    def "test for empty time series"() {
        given:
        def analysisResult = new FunctionValueMap(1, 1, 1)
        when:
        new Sum().execute(new MetricTimeSeries.Builder("Empty").build(), analysisResult)
        then:
        analysisResult.getAggregationValue(0) == Double.NaN
    }

    def "test arguments"() {
        expect:
        new Sum().getArguments().length == 0
    }

    def "test type"() {
        expect:
        new Sum().getQueryName() == "sum"
    }

    def "test equals and hash code"() {
        expect:
        def sum = new Sum()
        !sum.equals(null)
        !sum.equals(new Object())
        sum.equals(sum)
        sum.equals(new Sum())
        new Sum().hashCode() == new Sum().hashCode()
    }
}
