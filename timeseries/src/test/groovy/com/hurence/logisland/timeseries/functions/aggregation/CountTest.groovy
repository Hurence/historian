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
package com.hurence.logisland.timeseries.functions.aggregation

import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.functions.FunctionValueMap
import spock.lang.Specification

/**
 * Unit test for the count aggregation
 * @author f.lautenschlager
 */
class CountTest extends Specification {

    def "test execute"() {
        given:
        MetricTimeSeries.Builder timeSeries = new MetricTimeSeries.Builder("Count","metric")
        10.times {
            timeSeries.point(it, it * 10)
        }
        timeSeries.point(11, 9999)
        MetricTimeSeries ts = timeSeries.build()

        def analysisResult = new FunctionValueMap(1, 1, 1);

        when:
        new Count().execute(ts, analysisResult)
        then:
        analysisResult.getAggregationValue(0) == 11d
    }


    def "test for empty time series"() {
        given:
        def analysisResult = new FunctionValueMap(1, 1, 1)
        when:
        new Count().execute(new MetricTimeSeries.Builder("Empty","metric").build(), analysisResult)
        then:
        analysisResult.getAggregationValue(0) == 0.0d
    }


    def "test arguments"() {
        expect:
        new Count().getArguments().length == 0
    }

    def "test type"() {
        expect:
        new Count().getQueryName() == "count"
    }

    def "test equals and hash code"() {
        expect:
        def count = new Count()
        !count.equals(null)
        !count.equals(new Object())
        count.equals(count)
        count.equals(new Count())
        new Count().hashCode() == new Count().hashCode()
    }
}
