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

import com.hurence.timeseries.modele.points.Point;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ProtoBufTimeSeriesSerializerWithQualityEmbedded2Test {

    private static final Logger logger = LoggerFactory.getLogger(ProtoBufTimeSeriesSerializerWithQualityEmbedded2Test.class);

    @Test
    public void test1() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValueAndQuality(1, 1.2d, 1.0f),
                Point.fromValueAndQuality(2, 1.0d, 1.0f),
                Point.fromValueAndQuality(3, 1.8d, 0.2f),
                Point.fromValueAndQuality(4, 1.3d, 0.1f),
                Point.fromValueAndQuality(5, 1.2d, 1.0f)
        );
        testThereIsNoInformationLost(expectedPoints);
    }

    private void testThereIsNoInformationLost(List<Point> points) throws IOException {
        long start = points.get(0).getTimestamp();
        long end = points.get(points.size() - 1).getTimestamp();
        byte[] compressedProtoPoints = ProtoBufTimeSeriesWithQualitySerializer.to(points.iterator(), 0, 0);
        List<Point> uncompressedPoints = ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoPoints),
                start, end
        );
        assertEquals(points, uncompressedPoints);
    }

    @Test
    public void testThatItDoesNotWorkIfInputPointsAreNotSorted() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValueAndQuality(3, 1.2d, 1.0f),
                Point.fromValueAndQuality(2, 1.0d, 1.0f),
                Point.fromValueAndQuality(1, 1.8d, 0.2f),
                Point.fromValueAndQuality(4, 1.3d, 0.1f),
                Point.fromValueAndQuality(5, 1.2d, 1.0f)
        );
        long start = expectedPoints.get(0).getTimestamp();
        long end = expectedPoints.get(expectedPoints.size() - 1).getTimestamp();
        byte[] compressedProtoPoints = ProtoBufTimeSeriesWithQualitySerializer.to(expectedPoints);
        List<Point> uncompressedPoints = ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoPoints),
                start, end
        );
        assertNotEquals(expectedPoints, uncompressedPoints);
    }

    @Test
    public void testWithPointWithSameTimeStamp() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValueAndQuality(1, 1.2d, 1.0f),
                Point.fromValueAndQuality(1, 1.0d, 1.0f),
                Point.fromValueAndQuality(3, 1.8d, 0.2f),
                Point.fromValueAndQuality(3, 1.3d, 0.1f),
                Point.fromValueAndQuality(5, 1.2d, 1.0f)
        );
        testThereIsNoInformationLost(expectedPoints);
    }

    @Test
    public void testWithPointWithDuplicate() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValueAndQuality(1, 1.0d , 1.0f),
                Point.fromValueAndQuality(1, 1.0d , 1.0f),
                Point.fromValueAndQuality(30, 2.0d, 0.2f),
                Point.fromValueAndQuality(30, 2.0d, 0.1f),
                Point.fromValueAndQuality(50, 1.2d, 1.0f)
        );
        testThereIsNoInformationLost(expectedPoints);
    }

}
