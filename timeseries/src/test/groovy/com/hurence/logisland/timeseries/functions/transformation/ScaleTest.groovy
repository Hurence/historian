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
package com.hurence.logisland.timeseries.functions.transformation

import com.hurence.logisland.timeseries.MetricTimeSeries
import com.hurence.logisland.timeseries.functions.FunctionValueMap
import spock.lang.Specification

import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Unit test for the value transformation
 * @author f.lautenschlager
 */
class ScaleTest extends Specification {

    def "test transform"() {
        given:
        def timeSeriesBuilder = new MetricTimeSeries.Builder("Scale","metric")
        def now = Instant.now()

        100.times {
            timeSeriesBuilder.point(now.plus(it, ChronoUnit.SECONDS).toEpochMilli(), it + 1)
        }

        def scale = new Scale(["2"] as String[])
        def timeSeries = timeSeriesBuilder.build()
        def analysisResult = new FunctionValueMap(1, 1, 1)

        when:
        scale.execute(timeSeries, analysisResult)

        then:
        100.times {
            timeSeries.getValue(it) == (it + 1) * 2
        }
    }

    def "test getType"() {
        when:
        def scale = new Scale(["2"] as String[])
        then:
        scale.getQueryName() == "scale"
    }

    def "test getArguments"() {
        when:
        def scale = new Scale(["2"] as String[])
        then:
        scale.getArguments()[0] == "value=2.0"
    }

    def "test equals and hash code"() {
        expect:
        def function = new Scale(["4"] as String[])
        !function.equals(null)
        !function.equals(new Object())
        function.equals(function)
        function.equals(new Scale(["4"] as String[]))
        new Scale(["4"] as String[]).hashCode() == new Scale(["4"] as String[]).hashCode()
        new Scale(["4"] as String[]).hashCode() != new Scale(["2"] as String[]).hashCode()
    }

    def "test string representation"() {
        expect:
        def string = new Scale(["4"] as String[]).toString()
        string.contains("value")
    }
}
