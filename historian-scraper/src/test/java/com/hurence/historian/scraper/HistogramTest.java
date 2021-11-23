/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.historian.scraper;

import org.junit.jupiter.api.Test;
import com.hurence.historian.scraper.types.*;

import static org.junit.jupiter.api.Assertions.*;

public class HistogramTest {
    @Test
    public void testBuild() {
        Histogram histogram;

        try {
            histogram = new Histogram.Builder().build();
            fail("Should have thrown exception because name is not set");
        } catch (IllegalArgumentException expected) {
        }

        histogram = new Histogram.Builder().setName("foo").setSampleCount(123).setSampleSum(0.5)
                .addBucket(0.25, 100).addBucket(1.0, 200)
                .addLabel("one", "111").build();
        assertEquals("foo", histogram.getName());
        assertEquals(123, histogram.getSampleCount());
        assertEquals(0.5, histogram.getSampleSum(), 0.001);
        assertEquals(2, histogram.getBuckets().size());
        assertEquals(0.25, histogram.getBuckets().get(0).getUpperBound(), 0.01);
        assertEquals(100, histogram.getBuckets().get(0).getCumulativeCount());
        assertEquals(1.0, histogram.getBuckets().get(1).getUpperBound(), 0.01);
        assertEquals(200, histogram.getBuckets().get(1).getCumulativeCount());
        assertEquals(1, histogram.getLabels().size());
        assertEquals("111", histogram.getLabels().get("one"));
    }
}
