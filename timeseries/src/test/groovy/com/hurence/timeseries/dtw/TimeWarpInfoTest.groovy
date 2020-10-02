/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Stan Salvador (stansalvador@hotmail.com), Philip Chan (pkc@cs.fit.edu), QAware GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.hurence.timeseries.dtw

import com.hurence.timeseries.dtw.TimeWarpInfo
import spock.lang.Specification

/**
 * Unit test for the warp path
 * @author f.lautenschlager
 */
class TimeWarpInfoTest extends Specification {

    def "test GetDistance"() {
        given:
        def warpInfo = new TimeWarpInfo(0, null, 0, 0)

        when:
        def dist = warpInfo.getDistance()

        then:
        dist == 0d

    }

    def "test GetNormalizedDistance"() {
        given:
        def warpInfo = new TimeWarpInfo(10, null, 50, 50)

        when:
        def normalizedDistance = warpInfo.getNormalizedDistance()

        then:
        normalizedDistance == 0.1d
    }

    def "test GetPath"() {
        given:
        def warpInfo = new TimeWarpInfo(10, null, 50, 50)

        when:
        def path = warpInfo.getPath()

        then:
        path == null
    }

    def "test toString"() {
        given:
        def colMajorCell = new TimeWarpInfo(0, null, 0, 0)

        when:
        def string = colMajorCell.toString()
        then:
        string.contains("distance")
        string.contains("path")
        string.contains("base")

    }

    def "test equals"() {
        given:
        def colMajorCell = new TimeWarpInfo(0, null, 0, 0)

        when:
        def equals = colMajorCell.equals(others)
        then:
        equals == result
        colMajorCell.equals(colMajorCell)//always true

        where:
        others << [new Object(), null, new TimeWarpInfo(0, null, 0, 0)]
        result << [false, false, true]
    }

    def "test hashCode"() {
        given:
        def colMajorCell = new TimeWarpInfo(0, null, 0, 0)

        when:
        def hash = colMajorCell.hashCode()
        then:
        hash == new TimeWarpInfo(0, null, 0, 0).hashCode()
        hash != new TimeWarpInfo(1, null, 0, 0).hashCode()
    }
}
