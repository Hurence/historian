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

/**
 * Unit test for the moving average transformation
 * @author f.lautenschlager
 */
class MovingAverageTest extends Specification {
    def "test transform with last window contains only one point"() {
        given:
        def timeSeriesBuilder = new MetricTimeSeries.Builder("Moving average","metric")
        def movAvg = new MovingAverage(["5", "SECONDS"] as String[])

        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:00.000Z"), 5)//0
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:01.000Z"), 4)//1
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:02.000Z"), 3)//2
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:03.000Z"), 8)//3
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:04.000Z"), 4)//4
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:05.000Z"), 6)//5
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:06.000Z"), 10)//6
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:07.000Z"), 31)//7
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:08.000Z"), 9)//8
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:09.000Z"), 2)//9
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:15.000Z"), 1)//10
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:15.500Z"), 8)//11
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:16.000Z"), 5)//12
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:30.000Z"), 99)//13
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:30.500Z"), 77)//14
        timeSeriesBuilder.point(dateOf("2016-05-23T10:52:00.500Z"), 0)//15

        def timeSeries = timeSeriesBuilder.build()
        def analysisResult = new FunctionValueMap(1, 1, 1)

        when:
        movAvg.execute(timeSeries, analysisResult)
        then:
        timeSeries.size() == 15
        timeSeries.getValue(0) == 5.0d
        timeSeries.getTime(0) == dateOf("2016-05-23T10:51:02.500Z")
        timeSeries.getValue(1) == 5.833333333333333d
        timeSeries.getTime(1) == dateOf("2016-05-23T10:51:03.500Z")
        timeSeries.getValue(2) == 10.333333333333334d
        timeSeries.getTime(2) == dateOf("2016-05-23T10:51:04.500Z")
        timeSeries.getValue(3) == 11.333333333333334d
        timeSeries.getTime(3) == dateOf("2016-05-23T10:51:05.500Z")
        timeSeries.getValue(4) == 10.333333333333334d
        timeSeries.getTime(4) == dateOf("2016-05-23T10:51:06.500Z")
        timeSeries.getValue(5) == 11.6d
        timeSeries.getTime(5) == dateOf("2016-05-23T10:51:07.000Z")
        timeSeries.getValue(6) == 13.0d
        timeSeries.getTime(6) == dateOf("2016-05-23T10:51:07.500Z")
        timeSeries.getValue(7) == 14d
        timeSeries.getTime(7) == dateOf("2016-05-23T10:51:08.000Z")
        timeSeries.getValue(8) == 5.5d
        timeSeries.getTime(8) == dateOf("2016-05-23T10:51:08.500Z")
        timeSeries.getValue(9) == 2.0d
        timeSeries.getTime(9) == dateOf("2016-05-23T10:51:09.000Z")
        timeSeries.getValue(10) == 4.666666666666667d
        timeSeries.getTime(10) == dateOf("2016-05-23T10:51:15.500Z")
        timeSeries.getValue(11) == 6.5d
        timeSeries.getTime(11) == dateOf("2016-05-23T10:51:15.750Z")
        timeSeries.getValue(12) == 5.0d
        timeSeries.getTime(12) == dateOf("2016-05-23T10:51:16.000Z")
        timeSeries.getValue(13) == 88.0d
        timeSeries.getTime(13) == dateOf("2016-05-23T10:51:30.250Z")
        timeSeries.getValue(14) == 0.0d
        timeSeries.getTime(14) == dateOf("2016-05-23T10:52:00.500Z")
    }

    def "test transform with last window contains several points"() {
        given:
        def timeSeriesBuilder = new MetricTimeSeries.Builder("Moving average","metric")
        def movAvg = new MovingAverage(["5", "SECONDS"] as String[])

        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:00.000Z"), 5)//0
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:01.000Z"), 4)//1
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:02.000Z"), 3)//2
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:03.000Z"), 8)//3
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:04.000Z"), 4)//4
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:05.000Z"), 6)//5
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:06.000Z"), 10)//6
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:07.000Z"), 31)//7
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:08.000Z"), 9)//8
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:09.000Z"), 2)//9
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:15.000Z"), 1)//10
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:15.500Z"), 8)//11
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:16.000Z"), 5)//12
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:30.000Z"), 99)//13
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:30.500Z"), 77)//14

        def timeSeries = timeSeriesBuilder.build()
        def analysisResult = new FunctionValueMap(1, 1, 1)

        when:
        movAvg.execute(timeSeries, analysisResult)
        then:
        timeSeries.size() == 14
        timeSeries.getValue(0) == 5.0d
        timeSeries.getTime(0) == dateOf("2016-05-23T10:51:02.500Z")
        timeSeries.getValue(1) == 5.833333333333333d
        timeSeries.getTime(1) == dateOf("2016-05-23T10:51:03.500Z")
        timeSeries.getValue(2) == 10.333333333333334d
        timeSeries.getTime(2) == dateOf("2016-05-23T10:51:04.500Z")
        timeSeries.getValue(3) == 11.333333333333334d
        timeSeries.getTime(3) == dateOf("2016-05-23T10:51:05.500Z")
        timeSeries.getValue(4) == 10.333333333333334d
        timeSeries.getTime(4) == dateOf("2016-05-23T10:51:06.500Z")
        timeSeries.getValue(5) == 11.6d
        timeSeries.getTime(5) == dateOf("2016-05-23T10:51:07.000Z")
        timeSeries.getValue(6) == 13.0d
        timeSeries.getTime(6) == dateOf("2016-05-23T10:51:07.500Z")
        timeSeries.getValue(7) == 14d
        timeSeries.getTime(7) == dateOf("2016-05-23T10:51:08.000Z")
        timeSeries.getValue(8) == 5.5d
        timeSeries.getTime(8) == dateOf("2016-05-23T10:51:08.500Z")
        timeSeries.getValue(9) == 2.0d
        timeSeries.getTime(9) == dateOf("2016-05-23T10:51:09.000Z")
        timeSeries.getValue(10) == 4.666666666666667d
        timeSeries.getTime(10) == dateOf("2016-05-23T10:51:15.500Z")
        timeSeries.getValue(11) == 6.5d
        timeSeries.getTime(11) == dateOf("2016-05-23T10:51:15.750Z")
        timeSeries.getValue(12) == 5.0d
        timeSeries.getTime(12) == dateOf("2016-05-23T10:51:16.000Z")
        timeSeries.getValue(13) == 88.0d
        timeSeries.getTime(13) == dateOf("2016-05-23T10:51:30.250Z")
    }

    def "test transform with gaps"() {
        given:
        def timeSeriesBuilder = new MetricTimeSeries.Builder("Moving average","metric")
        def movAvg = new MovingAverage(["5", "SECONDS"] as String[])

        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:00.000Z"), 5)//0
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:10.000Z"), 4)//1
        timeSeriesBuilder.point(dateOf("2016-05-23T10:51:50.000Z"), 3)//2
        timeSeriesBuilder.point(dateOf("2016-05-23T10:52:00.000Z"), 8)//3
        timeSeriesBuilder.point(dateOf("2016-05-23T10:52:04.000Z"), 4)//4

        def timeSeries = timeSeriesBuilder.build()
        def analysisResult = new FunctionValueMap(1, 1, 1);

        when:
        movAvg.execute(timeSeries, analysisResult)
        then:
        timeSeries.size() == 4
        timeSeries.getValue(0) == 5.0d
        timeSeries.getTime(0) == dateOf("2016-05-23T10:51:00.000Z")
        timeSeries.getValue(1) == 4.0d
        timeSeries.getTime(1) == dateOf("2016-05-23T10:51:10.000Z")
        timeSeries.getValue(2) == 3.0d
        timeSeries.getTime(2) == dateOf("2016-05-23T10:51:50.000Z")
        timeSeries.getValue(3) == 6.0d
        timeSeries.getTime(3) == dateOf("2016-05-23T10:52:02.000Z")
    }


    long dateOf(format) {
        Instant.parse(format as String).toEpochMilli()
    }

    def "test getType"() {
        when:
        def movAvg = new MovingAverage(["4", "SECONDS"] as String[])

        then:
        movAvg.getQueryName() == "movavg"
    }

    def "test getArguments"() {
        when:
        def movAvg = new MovingAverage(["4", "DAYS"] as String[])

        then:
        movAvg.getArguments()[0] == "timeSpan=4"
        movAvg.getArguments()[1] == "unit=DAYS"
    }

    def "test toString"() {
        expect:
        def stringRepresentation = new MovingAverage(["4", "DAYS"] as String[]).toString()
        stringRepresentation.contains("timeSpan")
        stringRepresentation.contains("unit")
    }

    def "test equals and hash code"() {
        expect:
        def function = new MovingAverage(["4", "DAYS"] as String[])
        !function.equals(null)
        !function.equals(new Object())
        function.equals(function)
        function.equals(new MovingAverage(["4", "DAYS"] as String[]))
        new MovingAverage(["4", "DAYS"] as String[]).hashCode() == new MovingAverage(["4", "DAYS"] as String[]).hashCode()
        new MovingAverage(["4", "DAYS"] as String[]).hashCode() != new MovingAverage(["2", "DAYS"] as String[]).hashCode()
        new MovingAverage(["4", "DAYS"] as String[]).hashCode() != new MovingAverage(["4", "SECONDS"] as String[]).hashCode()
    }
}
