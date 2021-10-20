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
import com.hurence.historian.scraper.types.Summary;

import static org.junit.jupiter.api.Assertions.*;

public class SummaryTest {
    @Test
    public void testBuild() {
        Summary summary;

        try {
            summary = new Summary.Builder().build();
            fail("Should have thrown exception because name is not set");
        } catch (IllegalArgumentException expected) {
        }

        summary = new Summary.Builder().setName("foo").setSampleCount(123).setSampleSum(0.5)
                .addQuantile(0.25, 100.1).addQuantile(0.75, 200.2)
                .addLabel("one", "111").build();
        assertEquals("foo", summary.getName());
        assertEquals(123, summary.getSampleCount());
        assertEquals(0.5, summary.getSampleSum(), 0.001);
        assertEquals(2, summary.getQuantiles().size());
        assertEquals(0.25, summary.getQuantiles().get(0).getQuantile(), 0.01);
        assertEquals(100.1, summary.getQuantiles().get(0).getValue(), 0.01);
        assertEquals(0.75, summary.getQuantiles().get(1).getQuantile(), 0.01);
        assertEquals(200.2, summary.getQuantiles().get(1).getValue(), 0.01);
        assertEquals(1, summary.getLabels().size());
        assertEquals("111", summary.getLabels().get("one"));
    }
}
