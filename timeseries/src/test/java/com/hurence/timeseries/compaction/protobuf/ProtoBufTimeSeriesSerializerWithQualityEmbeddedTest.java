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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
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
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.2d),
                Measure.fromValue(2, 1.0d),
                Measure.fromValue(3, 1.8d),
                Measure.fromValue(4, 1.3d),
                Measure.fromValue(5, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, ddc);
    }

    private void testThereIsNoInformationLost(List<Measure> Measures, int ddc) throws IOException {
        long start = Measures.get(0).getTimestamp();
        long end = Measures.get(Measures.size() - 1).getTimestamp();
        byte[] compressedProtoMeasures = ProtoBufTimeSeriesWithQualitySerializer.to(Measures, ddc);
        TreeSet<Measure> uncompressedMeasures = ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoMeasures),
                start, end
        );
        assertEquals(new TreeSet<>(Measures), uncompressedMeasures);
    }

    @Test
    public void testThatItDoesNotWorkIfInputMeasuresAreNotSorted() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(3, 1.2d),
                Measure.fromValue(2, 1.0d),
                Measure.fromValue(1, 1.8d),
                Measure.fromValue(4, 1.3d),
                Measure.fromValue(5, 1.2d)
        );
        long start = expectedMeasures.get(0).getTimestamp();
        long end = expectedMeasures.get(expectedMeasures.size() - 1).getTimestamp();
        byte[] compressedProtoMeasures = ProtoBufTimeSeriesWithQualitySerializer.to(expectedMeasures, 0);
        TreeSet<Measure> uncompressedMeasures = ProtoBufTimeSeriesWithQualitySerializer.from(
                new ByteArrayInputStream(compressedProtoMeasures),
                start, end
        );
        assertNotEquals(new TreeSet<>(expectedMeasures), uncompressedMeasures);
    }

    @Test
    public void testWithMeasureWithSameTimeStamp() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.2d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(3, 1.8d),
                Measure.fromValue(3, 1.3d),
                Measure.fromValue(5, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, 0);
    }

    @Test
    public void testWithMeasureWithDuplicate() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, 0);
    }

    @Test
    public void testWithMeasureWithDuplicateAndDdc1() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, 1);
    }

    @Test
    public void testWithMeasureWithDuplicateDoubleAndDdc1() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(30, 1.8d),
                Measure.fromValue(30, 1.8d),
                Measure.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, 2);
    }

    @Test
    public void testWithMeasureWithDuplicateAndDdc2() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(50, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, 2);
    }

    @Test
    public void testWithMeasureWithDuplicateAndDdc2_2() throws IOException {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d)
        );
        testThereIsNoInformationLost(expectedMeasures, 2);
    }


    @Test
    public void testCompressionFactor() {
        List<Measure> expectedMeasures = Arrays.asList(
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(1, 1.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(30, 2.0d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(50, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d),
                Measure.fromValue(500, 1.2d)
        );
        compareCompressionRate(expectedMeasures);
    }

    @Test
    public void testCompressionFactorWithIncreasingTimestampAndValue() {
        List<Measure> expectedMeasures = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Measure.fromValue(l * 10, (double)l * 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedMeasures, 0, 10);
        compareCompressionRate(expectedMeasures, 0, 100);
        compareCompressionRate(expectedMeasures, 0, 1000);
        compareCompressionRate(expectedMeasures, 0, 10000);
        compareCompressionRate(expectedMeasures, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithIncreasingTimestampButConstantValue() {
        List<Measure> expectedMeasures = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Measure.fromValue(l * 10, 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedMeasures, 0, 10);
        compareCompressionRate(expectedMeasures, 0, 100);
        compareCompressionRate(expectedMeasures, 0, 1000);
        compareCompressionRate(expectedMeasures, 0, 10000);
        compareCompressionRate(expectedMeasures, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithIncreasingValueButConstantTimestamp() {
        List<Measure> expectedMeasures = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Measure.fromValue(15487484566L, (double)l * 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedMeasures, 0, 10);
        compareCompressionRate(expectedMeasures, 0, 100);
        compareCompressionRate(expectedMeasures, 0, 1000);
        compareCompressionRate(expectedMeasures, 0, 10000);
        compareCompressionRate(expectedMeasures, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithDuplicates() {
        List<Measure> expectedMeasures = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    return Measure.fromValue(15487484566L, 1.5d);
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedMeasures, 0, 10);
        compareCompressionRate(expectedMeasures, 0, 100);
        compareCompressionRate(expectedMeasures, 0, 1000);
        compareCompressionRate(expectedMeasures, 0, 10000);
        compareCompressionRate(expectedMeasures, 0, 100000);
    }

    @Test
    public void testCompressionFactorWithSeveralDuplicates() {
        List<Measure> expectedMeasures = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Measure.fromValue(10, 1.5d);
                    } else if (l < 100) {
                        return Measure.fromValue(100, 1.5d);
                    }else if (l < 200) {
                        return Measure.fromValue(1000, 1.5d);
                    }else if (l < 300) {
                        return Measure.fromValue(2000, 1.5d);
                    }else if (l < 400) {
                        return Measure.fromValue(3000, 1.5d);
                    }else if (l < 500) {
                        return Measure.fromValue(3300, 1.5d);
                    }else if (l < 600) {
                        return Measure.fromValue(3500, 1.5d);
                    }else if (l < 800) {
                        return Measure.fromValue(4000, 1.5d);
                    }else if (l < 900) {
                        return Measure.fromValue(5000, 1.5d);
                    } else {
                        return Measure.fromValue(6000, 1.5d);
                    }
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedMeasures, 0, 10);
        compareCompressionRate(expectedMeasures, 0, 100);
        compareCompressionRate(expectedMeasures, 0, 1000);
        compareCompressionRate(expectedMeasures, 0, 10000);
        compareCompressionRate(expectedMeasures, 0, 100000);
    }
    @Test
    public void testCompressionFactorWithSeveralDuplicates_2() {
        List<Measure> expectedMeasures = LongStream.range(0, 1000)
                .mapToObj(l -> {
                    if (l < 50) {
                        return Measure.fromValue(10, 1.5d);
                    } else if (l < 100) {
                        return Measure.fromValue(100, 3d);
                    }else if (l < 200) {
                        return Measure.fromValue(1000, 50d);
                    }else if (l < 300) {
                        return Measure.fromValue(2000, 50.5d);
                    }else if (l < 400) {
                        return Measure.fromValue(3000, 50.6d);
                    }else if (l < 500) {
                        return Measure.fromValue(3300, 50.7d);
                    }else if (l < 600) {
                        return Measure.fromValue(3500, 49.5d);
                    }else if (l < 800) {
                        return Measure.fromValue(4000, 1.5d);
                    }else if (l < 900) {
                        return Measure.fromValue(5000, 2d);
                    } else {
                        return Measure.fromValue(6000, 80d);
                    }
                })
                .collect(Collectors.toList());
        compareCompressionRate(expectedMeasures, 0, 10);
        compareCompressionRate(expectedMeasures, 0, 100);
        compareCompressionRate(expectedMeasures, 0, 1000);
        compareCompressionRate(expectedMeasures, 0, 10000);
        compareCompressionRate(expectedMeasures, 0, 100000);
    }

    private void compareCompressionRate(List<Measure> expectedMeasures) {
        compareCompressionRate(expectedMeasures, 0, 1000);
    }

    private void compareCompressionRate(List<Measure> expectedMeasures, int ddc1, int ddc2) {
        byte[] compressed1 = ProtoBufTimeSeriesWithQualitySerializer.to(expectedMeasures, ddc1);
        logger.info("compression with ddc {} is {}",ddc1,  compressed1.length);
        byte[] compressed2 = ProtoBufTimeSeriesWithQualitySerializer.to(expectedMeasures, ddc2);
        logger.info("compression with ddc {} is {}", ddc2, compressed2.length);
        double rateGain = BigDecimal.valueOf(compressed2.length)
                .divide(BigDecimal.valueOf(compressed1.length), 2, RoundingMode.HALF_EVEN)
                .multiply(BigDecimal.valueOf(100).setScale(2, RoundingMode.HALF_EVEN))
                .doubleValue();
        logger.info("So we gained {}%", 100 - rateGain);
    }


}
