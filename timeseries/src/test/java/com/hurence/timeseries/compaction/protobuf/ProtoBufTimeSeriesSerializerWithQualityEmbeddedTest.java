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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Should test equivalence between this algo and the first historically
 */
public class ProtoBufTimeSeriesSerializerWithQualityEmbeddedTest {

    private static final Logger logger = LoggerFactory.getLogger(ProtoBufTimeSeriesSerializerWithQualityEmbeddedTest.class);

    @Test
    public void test1() throws IOException  {
        List<Integer> ddcs = IntStream.range(0, 10)
                .boxed()
                .collect(Collectors.toList());

        for (int ddc : ddcs) {
            this.runWithDdc(ddc);
        }
    }

    private void runWithDdc(int ddc) throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.2d),
                Point.fromValue(2, 1.0d),
                Point.fromValue(3, 1.8d),
                Point.fromValue(4, 1.3d),
                Point.fromValue(5, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, ddc);
    }

    private void testThereIsNoInformationLost(List<Point> points, int ddc) throws IOException {
        long start = points.get(0).getTimestamp();
        long end = points.get(points.size() - 1).getTimestamp();
        byte[] compressedProtoPoints = ProtoBufTimeSeriesWithQualitySerializer.to(points, ddc);
        List<Point> uncompressedPoints = ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoPoints),
                start, end
        );
        assertEquals(points, uncompressedPoints);
    }

    @Test
    public void testThatItDoesNotWorkIfInputPointsAreNotSorted() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(3, 1.2d),
                Point.fromValue(2, 1.0d),
                Point.fromValue(1, 1.8d),
                Point.fromValue(4, 1.3d),
                Point.fromValue(5, 1.2d)
        );
        long start = expectedPoints.get(0).getTimestamp();
        long end = expectedPoints.get(expectedPoints.size() - 1).getTimestamp();
        byte[] compressedProtoPoints = ProtoBufTimeSeriesWithQualitySerializer.to(expectedPoints, 0);
        List<Point> uncompressedPoints = ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoPoints),
                start, end
        );
        assertNotEquals(expectedPoints, uncompressedPoints);
    }

    @Test
    public void testWithPointWithSameTimeStamp() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.2d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(3, 1.8d),
                Point.fromValue(3, 1.3d),
                Point.fromValue(5, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, 0);
    }

    @Test
    public void testWithPointWithDuplicate() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, 0);
    }

    @Test
    public void testWithPointWithDuplicateAndDdc1() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, 1);
    }

    @Test
    public void testWithPointWithDuplicateDoubleAndDdc1() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(30, 1.8d),
                Point.fromValue(30, 1.8d),
                Point.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, 2);
    }

    @Test
    public void testWithPointWithDuplicateAndDdc2() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, 2);
    }

    @Test
    public void testWithPointWithDuplicateAndDdc2_2() throws IOException {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d)
        );
        testThereIsNoInformationLost(expectedPoints, 2);
    }


    @Test
    public void testCompressionFactor() {
        List<Point> expectedPoints = Arrays.asList(
                Point.fromValue(1, 1.0d),
                Point.fromValue(1, 1.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(30, 2.0d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(50, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d),
                Point.fromValue(500, 1.2d)
        );
        compareCompressionRate(expectedPoints);
    }

    @Test
    public void testCompressionFactorWithIncreasingTimestampAndValue() {
        List<Point> expectedPoints = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Point.fromValue(l * 10, (double)l * 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedPoints, 0, 10);
        compareCompressionRate(expectedPoints, 0, 100);
        compareCompressionRate(expectedPoints, 0, 1000);
        compareCompressionRate(expectedPoints, 0, 10000);
        compareCompressionRate(expectedPoints, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithIncreasingTimestampButConstantValue() {
        List<Point> expectedPoints = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Point.fromValue(l * 10, 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedPoints, 0, 10);
        compareCompressionRate(expectedPoints, 0, 100);
        compareCompressionRate(expectedPoints, 0, 1000);
        compareCompressionRate(expectedPoints, 0, 10000);
        compareCompressionRate(expectedPoints, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithIncreasingValueButConstantTimestamp() {
        List<Point> expectedPoints = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Point.fromValue(15487484566L, (double)l * 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedPoints, 0, 10);
        compareCompressionRate(expectedPoints, 0, 100);
        compareCompressionRate(expectedPoints, 0, 1000);
        compareCompressionRate(expectedPoints, 0, 10000);
        compareCompressionRate(expectedPoints, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithDuplicates() {
        List<Point> expectedPoints = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Point.fromValue(15487484566L, 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedPoints, 0, 10);
        compareCompressionRate(expectedPoints, 0, 100);
        compareCompressionRate(expectedPoints, 0, 1000);
        compareCompressionRate(expectedPoints, 0, 10000);
        compareCompressionRate(expectedPoints, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithSeveralDuplicates() {
        List<Point> expectedPoints = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Point.fromValue(10, 1.5d);
                    } else if (l < 100) {
                        return Point.fromValue(100, 1.5d);
                    }else if (l < 200) {
                        return Point.fromValue(1000, 1.5d);
                    }else if (l < 300) {
                        return Point.fromValue(2000, 1.5d);
                    }else if (l < 400) {
                        return Point.fromValue(3000, 1.5d);
                    }else if (l < 500) {
                        return Point.fromValue(3300, 1.5d);
                    }else if (l < 600) {
                        return Point.fromValue(3500, 1.5d);
                    }else if (l < 800) {
                        return Point.fromValue(4000, 1.5d);
                    }else if (l < 900) {
                        return Point.fromValue(5000, 1.5d);
                    } else {
                        return Point.fromValue(6000, 1.5d);
                    }
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedPoints, 0, 10);
        compareCompressionRate(expectedPoints, 0, 100);
        compareCompressionRate(expectedPoints, 0, 1000);
        compareCompressionRate(expectedPoints, 0, 10000);
        compareCompressionRate(expectedPoints, 0, 100000);
    }
    @Test
    public void testCompressionFactorWithSeveralDuplicates_2() {
        List<Point> expectedPoints = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Point.fromValue(10, 1.5d);
                    } else if (l < 100) {
                        return Point.fromValue(100, 3d);
                    }else if (l < 200) {
                        return Point.fromValue(1000, 50d);
                    }else if (l < 300) {
                        return Point.fromValue(2000, 50.5d);
                    }else if (l < 400) {
                        return Point.fromValue(3000, 50.6d);
                    }else if (l < 500) {
                        return Point.fromValue(3300, 50.7d);
                    }else if (l < 600) {
                        return Point.fromValue(3500, 49.5d);
                    }else if (l < 800) {
                        return Point.fromValue(4000, 1.5d);
                    }else if (l < 900) {
                        return Point.fromValue(5000, 2d);
                    } else {
                        return Point.fromValue(6000, 80d);
                    }
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedPoints, 0, 10);
        compareCompressionRate(expectedPoints, 0, 100);
        compareCompressionRate(expectedPoints, 0, 1000);
        compareCompressionRate(expectedPoints, 0, 10000);
        compareCompressionRate(expectedPoints, 0, 100000);
    }

    private void compareCompressionRate(List<Point> expectedPoints) {
        compareCompressionRate(expectedPoints, 0, 1000);
    }

    private void compareCompressionRate(List<Point> expectedPoints, int ddc1, int ddc2) {
        byte[] compressed1 = ProtoBufTimeSeriesWithQualitySerializer.to(expectedPoints, ddc1);
        logger.info("compression with ddc {} is {}",ddc1,  compressed1.length);
        byte[] compressed2 = ProtoBufTimeSeriesWithQualitySerializer.to(expectedPoints, ddc2);
        logger.info("compression with ddc {} is {}", ddc2, compressed2.length);
        double rateGain = BigDecimal.valueOf(compressed2.length)
                .divide(BigDecimal.valueOf(compressed1.length), 2, RoundingMode.HALF_EVEN)
                .multiply(BigDecimal.valueOf(100).setScale(2, RoundingMode.HALF_EVEN))
                .doubleValue();
        logger.info("So we gained {}%", 100 - rateGain);
    }


}
