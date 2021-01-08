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
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerializerCompatibilityTest {

    private static final Logger logger = LoggerFactory.getLogger(SerializerCompatibilityTest.class);

    @Test
    public void testCompression1() throws IOException {
        List<Measure> expectedMeasures = LongStream.range(0, 10000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Measure.fromValue(10, 1.5d);
                    } else if (l < 1000) {
                        return Measure.fromValue(100, 3d);
                    }else if (l < 2000) {
                        return Measure.fromValue(1000, 50d);
                    }else if (l < 3000) {
                        return Measure.fromValue(2000, 50.5d);
                    }else if (l < 4000) {
                        return Measure.fromValue(3000, 50.6d);
                    }else if (l < 5000) {
                        return Measure.fromValue(3300, 50.7d);
                    }else if (l < 6000) {
                        return Measure.fromValue(3500, 49.5d);
                    }else if (l < 8000) {
                        return Measure.fromValue(4000, 1.5d);
                    }else if (l < 9000) {
                        return Measure.fromValue(5000, 2d);
                    } else {
                        return Measure.fromValue(l, 80d);
                    }
                })
                .collect(Collectors.toList());
        byte[] compressedOlgAlgo = ProtoBufTimeSeriesCurrentSerializer.to(expectedMeasures);
        long start = expectedMeasures.get(0).getTimestamp();
        long end = expectedMeasures.get(expectedMeasures.size() - 1).getTimestamp();
        TreeSet<Measure> uncompressedMeasures = ProtoBufTimeSeriesCurrentSerializer.from(
                new ByteArrayInputStream(compressedOlgAlgo),
                start, end, compressedOlgAlgo
        );
        assertEquals(new TreeSet<Measure>(expectedMeasures), new TreeSet<>(uncompressedMeasures));
    }


}
