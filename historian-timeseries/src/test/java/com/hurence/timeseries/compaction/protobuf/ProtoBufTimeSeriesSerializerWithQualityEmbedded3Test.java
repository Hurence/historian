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
package com.hurence.timeseries.compaction.protobuf;


import com.hurence.timeseries.model.Measure;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * This test focus on testing that we are not saving almost equals floats.
 *
 * The actual behavior is to trigger a storage of quality if the diff between two qualities > threshhold
 */
public class ProtoBufTimeSeriesSerializerWithQualityEmbedded3Test {

    private static final Logger logger = LoggerFactory.getLogger(ProtoBufTimeSeriesSerializerWithQualityEmbedded3Test.class);

    @Test
    public void test1() throws IOException {
        List<Measure> inputMeasures = Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.0f),
                Measure.fromValueAndQuality(3, 1.8d, 0.2f),
                Measure.fromValueAndQuality(3, 1.8d, 0.10000001f),
                Measure.fromValueAndQuality(4, 1.3d, 0.1f),
                Measure.fromValueAndQuality(5, 1.2d, 1.0f)
        );
        TreeSet<Measure> actualMeasures = testThereIsNoInformationLost(inputMeasures, 0.09f);
        TreeSet<Measure> expectedMeasures = new TreeSet<>(Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.0f),
                Measure.fromValueAndQuality(3, 1.8d, 0.2f),
                Measure.fromValueAndQuality(3, 1.8d, 0.10000001f),
                Measure.fromValueAndQuality(4, 1.3d, 0.10000001f),
                Measure.fromValueAndQuality(5, 1.2d, 1.0f)
        ));
        assertEquals(expectedMeasures, actualMeasures);
    }

    @Test
    public void test2() throws IOException {
        List<Measure> inputMeasures = Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.5f),
                Measure.fromValueAndQuality(3, 1.8d, 1.549f),
                Measure.fromValueAndQuality(4, 1.3d, 1.451f),
                Measure.fromValueAndQuality(5, 1.2d, 1.45f)
        );
        TreeSet<Measure> actualMeasures = testThereIsNoInformationLost(inputMeasures, 0.049f);
        TreeSet<Measure> expectedMeasures = new TreeSet(Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.5f),
                Measure.fromValueAndQuality(3, 1.8d, 1.549f),
                Measure.fromValueAndQuality(4, 1.3d, 1.451f),
                Measure.fromValueAndQuality(5, 1.2d, 1.451f)
        ));
        assertEquals(expectedMeasures, actualMeasures);
    }

    //Here when the diff is exactly 0.05 this is not enough to make the quality stored
    @Test
    public void test3() throws IOException {
        List<Measure> inputMeasures = Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 0.9f),
                Measure.fromValueAndQuality(3, 1.0d, 0.8f),
                Measure.fromValueAndQuality(4, 1.0d, 0.7f),
                Measure.fromValueAndQuality(5, 1.0d, 0.69f),
                Measure.fromValueAndQuality(6, 1.0d, 0.68f),
                Measure.fromValueAndQuality(7, 1.0d, 0.679f),
                Measure.fromValueAndQuality(8, 1.0d, 0.671f),
                Measure.fromValueAndQuality(9, 1.0d, 0.67f),
                Measure.fromValueAndQuality(10, 1.0d, 0.669f),
                Measure.fromValueAndQuality(11, 1.0d, 0.05f),
                Measure.fromValueAndQuality(12, 1.0d, 0.0f),
                Measure.fromValueAndQuality(13, 1.2d, 0.9f),
                Measure.fromValueAndQuality(14, 1.2d, 0.99f),
                Measure.fromValueAndQuality(15, 1.2d, 1f)
        );
        TreeSet<Measure> actualMeasures = testThereIsNoInformationLost(inputMeasures, 0.33f);
        TreeSet<Measure> expectedMeasures = new TreeSet(Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.0f),
                Measure.fromValueAndQuality(3, 1.0d, 1.0f),
                Measure.fromValueAndQuality(4, 1.0d, 1.0f),
                Measure.fromValueAndQuality(5, 1.0d, 1.0f),
                Measure.fromValueAndQuality(6, 1.0d, 1.0f),
                Measure.fromValueAndQuality(7, 1.0d, 1.0f),
                Measure.fromValueAndQuality(8, 1.0d, 1.0f),
                Measure.fromValueAndQuality(9, 1.0d, 1.0f),
                Measure.fromValueAndQuality(10, 1.0d, 0.669f),
                Measure.fromValueAndQuality(11, 1.0d, 0.05f),
                Measure.fromValueAndQuality(12, 1.0d, 0.0f),
                Measure.fromValueAndQuality(13, 1.2d, 0.9f),
                Measure.fromValueAndQuality(14, 1.2d, 0.9f),
                Measure.fromValueAndQuality(15, 1.2d, 1f)
        ));
        assertEquals(expectedMeasures, actualMeasures);
    }

    //Here when the diff is exactly 0.05 this is not enough to make the quality stored
    @Test
    public void test4() throws IOException {
        List<Measure> inputMeasures = Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.0f),
                Measure.fromValueAndQuality(3, 1.8d, 0.2f),
                Measure.fromValueAndQuality(4, 1.8d, 0.10000001f),
                Measure.fromValueAndQuality(5, 1.3d, 0.1f),
                Measure.fromValueAndQuality(6, 1.3d, 0.09f),
                Measure.fromValueAndQuality(7, 1.2d, 1.0f)
        );
        TreeSet<Measure> actualMeasures = testThereIsNoInformationLost(inputMeasures, 0.1f);
        TreeSet<Measure> expectedMeasures = new TreeSet(Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.0f),
                Measure.fromValueAndQuality(3, 1.8d, 0.2f),
                Measure.fromValueAndQuality(4, 1.8d, 0.2f),
                Measure.fromValueAndQuality(5, 1.3d, 0.2f),
                Measure.fromValueAndQuality(6, 1.3d, 0.09f),
                Measure.fromValueAndQuality(7, 1.2d, 1.0f)
        ));
        assertEquals(expectedMeasures, actualMeasures);
    }

    //Here when the diff is exactly 0.05 this is not enough to make the quality stored
    @Test
    public void test5() throws IOException {
        List<Measure> inputMeasures = Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.5f),
                Measure.fromValueAndQuality(3, 1.8d, 1.549f),
                Measure.fromValueAndQuality(4, 1.3d, 1.451f),
                Measure.fromValueAndQuality(5, 1.2d, 1.45f),
                Measure.fromValueAndQuality(6, 1.2d, 1.449f)
        );
        TreeSet<Measure> actualMeasures = testThereIsNoInformationLost(inputMeasures, 0.05f);
        TreeSet<Measure> expectedMeasures = new TreeSet(Arrays.asList(
                Measure.fromValueAndQuality(1, 1.2d, 1.0f),
                Measure.fromValueAndQuality(2, 1.0d, 1.5f),
                Measure.fromValueAndQuality(3, 1.8d, 1.5f),
                Measure.fromValueAndQuality(4, 1.3d, 1.5f),
                Measure.fromValueAndQuality(5, 1.2d, 1.5f),
                Measure.fromValueAndQuality(6, 1.2d, 1.449f)
        ));
        assertEquals(expectedMeasures, actualMeasures);
    }

    private TreeSet<Measure> testThereIsNoInformationLost(List<Measure> Measures, float diffAcceptedForQuality) throws IOException {
        long start = Measures.get(0).getTimestamp();
        long end = Measures.get(Measures.size() - 1).getTimestamp();
        byte[] compressedProtoMeasures = ProtoBufTimeSeriesWithQualitySerializer.to(Measures.iterator(), diffAcceptedForQuality,0);
        return ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoMeasures),
                start, end
        );
    }

}