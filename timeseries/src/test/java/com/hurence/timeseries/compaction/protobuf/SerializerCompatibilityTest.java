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

import com.hurence.timeseries.modele.Point;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerializerCompatibilityTest {

    private static final Logger logger = LoggerFactory.getLogger(SerializerCompatibilityTest.class);

    @Test
    public void testCompression1() throws IOException {
        List<Point> expectedPoints = LongStream.range(0, 10000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Point.fromValue(10, 1.5d);
                    } else if (l < 1000) {
                        return Point.fromValue(100, 3d);
                    }else if (l < 2000) {
                        return Point.fromValue(1000, 50d);
                    }else if (l < 3000) {
                        return Point.fromValue(2000, 50.5d);
                    }else if (l < 4000) {
                        return Point.fromValue(3000, 50.6d);
                    }else if (l < 5000) {
                        return Point.fromValue(3300, 50.7d);
                    }else if (l < 6000) {
                        return Point.fromValue(3500, 49.5d);
                    }else if (l < 8000) {
                        return Point.fromValue(4000, 1.5d);
                    }else if (l < 9000) {
                        return Point.fromValue(5000, 2d);
                    } else {
                        return Point.fromValue(l, 80d);
                    }
                })
                .collect(Collectors.toList());
        byte[] compressedOlgAlgo = ProtoBufTimeSeriesSerializer.to(expectedPoints);
        long start = expectedPoints.get(0).getTimestamp();
        long end = expectedPoints.get(expectedPoints.size() - 1).getTimestamp();
        List<Point> uncompressedPoints = ProtoBufTimeSeriesCurrentSerializer.from(
                new ByteArrayInputStream(compressedOlgAlgo),
                start, end
        );
        assertEquals(expectedPoints, uncompressedPoints);
    }


}
