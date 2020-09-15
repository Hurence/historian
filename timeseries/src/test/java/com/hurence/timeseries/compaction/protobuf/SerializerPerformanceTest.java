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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class SerializerPerformanceTest {

    private static final Logger logger = LoggerFactory.getLogger(SerializerPerformanceTest.class);

    @Test
    public void testCompression1() {
        List<Point> expectedPoints = LongStream.range(0, 10000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Point.fromValueAndQuality(10, 1.5d, 0f);
                    } else if (l < 1000) {
                        return Point.fromValueAndQuality(100, 3d, 0.2f);
                    }else if (l < 2000) {
                        return Point.fromValueAndQuality(1000, 50d, 0.4f);
                    }else if (l < 3000) {
                        return Point.fromValueAndQuality(2000, 50.5d, 0.6f);
                    }else if (l < 4000) {
                        return Point.fromValueAndQuality(3000, 50.6d, 0.8f);
                    }else if (l < 5000) {
                        return Point.fromValueAndQuality(3300, 50.7d, 0.9f);
                    }else if (l < 6000) {
                        return Point.fromValueAndQuality(3500, 49.5d, 0.95f);
                    }else if (l < 8000) {
                        return Point.fromValueAndQuality(4000, 1.5d, 1f);
                    }else if (l < 9000) {
                        return Point.fromValueAndQuality(5000, 2d, 1f);
                    } else {
                        return Point.fromValueAndQuality(l, 80d, 1f);
                    }
                })
                .collect(Collectors.toList());
        compressPoints(expectedPoints);
    }

    @Test
    public void testCompression2() {
        List<Point> expectedPoints = LongStream.range(0, 10000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Point.fromValueAndQuality(l, 1.5d, 0f);
                    } else if (l < 1000) {
                        return Point.fromValueAndQuality(l, 3d, 0.2f);
                    }else if (l < 2000) {
                        return Point.fromValueAndQuality(l, 50d, 0.4f);
                    }else if (l < 3000) {
                        return Point.fromValueAndQuality(l, 50.5d, 0.6f);
                    }else if (l < 4000) {
                        return Point.fromValueAndQuality(l, 50.6d, 0.8f);
                    }else if (l < 5000) {
                        return Point.fromValueAndQuality(l, 50.7d, 0.9f);
                    }else if (l < 6000) {
                        return Point.fromValueAndQuality(l, 49.5d, 0.95f);
                    }else if (l < 8000) {
                        return Point.fromValueAndQuality(l, 1.5d, 1f);
                    }else if (l < 9000) {
                        return Point.fromValueAndQuality(l, 2d, 1f);
                    } else {
                        return Point.fromValueAndQuality(l, 80d, 1f);
                    }
                })
                .collect(Collectors.toList());
        compressPoints(expectedPoints);
    }

    @Test
    public void testCompressionWithConstantQuality() {
        final float quality = 1f;
        List<Point> expectedPoints = LongStream.range(0, 100000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Point.fromValueAndQuality(l, 1.5d, quality);
                    } else if (l < 1000) {
                        return Point.fromValueAndQuality(l, 3d, quality);
                    }else if (l < 2000) {
                        return Point.fromValueAndQuality(l, 50d, quality);
                    }else if (l < 3000) {
                        return Point.fromValueAndQuality(l, 50.5d, quality);
                    }else if (l < 4000) {
                        return Point.fromValueAndQuality(l, 50.6d, quality);
                    }else if (l < 5000) {
                        return Point.fromValueAndQuality(l, 50.7d, quality);
                    }else if (l < 6000) {
                        return Point.fromValueAndQuality(l, 49.5d, quality);
                    }else if (l < 8000) {
                        return Point.fromValueAndQuality(l, 1.5d, quality);
                    }else if (l < 9000) {
                        return Point.fromValueAndQuality(l, 2d, quality);
                    } else {
                        return Point.fromValueAndQuality(l, 80d, quality);
                    }
                })
                .collect(Collectors.toList());
        compressPoints(expectedPoints);
    }

    @Test
    public void testCompressionWithAlwaysDifferentQuality() {
        List<Point> expectedPoints = LongStream.range(0, 10000)
                .mapToObj(l -> {
                    String floatStr = "" + l;
                    final float quality = Float.parseFloat(floatStr);
                    if (l < 50) {
                        return Point.fromValueAndQuality(l, 1.5d, quality);
                    } else if (l < 1000) {
                        return Point.fromValueAndQuality(l, 3d, quality);
                    }else if (l < 2000) {
                        return Point.fromValueAndQuality(l, 50d, quality);
                    }else if (l < 3000) {
                        return Point.fromValueAndQuality(l, 50.5d, quality);
                    }else if (l < 4000) {
                        return Point.fromValueAndQuality(l, 50.6d, quality);
                    }else if (l < 5000) {
                        return Point.fromValueAndQuality(l, 50.7d, quality);
                    }else if (l < 6000) {
                        return Point.fromValueAndQuality(l, 49.5d, quality);
                    }else if (l < 8000) {
                        return Point.fromValueAndQuality(l, 1.5d, quality);
                    }else if (l < 9000) {
                        return Point.fromValueAndQuality(l, 2d, quality);
                    } else {
                        return Point.fromValueAndQuality(l, 80d, quality);
                    }
                })
                .collect(Collectors.toList());
        compressPoints(expectedPoints);
    }

    private void compressPoints(List<Point> expectedPoints) {
        byte[] compressed1 = ProtoBufTimeSeriesWithQualitySerializer.to(expectedPoints);
        logger.info("compression with quality length is {}",  compressed1.length);
        byte[] compressedWithoutQuality = ProtoBufTimeSeriesSerializer.to(
                expectedPoints.stream().map(Point::withoutQuality).collect(Collectors.toList())
        );
        logger.info("compression without quality length is {}",  compressedWithoutQuality.length);
    }
}
