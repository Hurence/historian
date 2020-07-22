package com.hurence.timeseries.compaction.protobuf;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

public class PointsCompressorWithQualityTest {

    private static final Logger logger = LoggerFactory.getLogger(PointsCompressorWithQualityTest.class);

    @Test
    public void test1()  {
        assertTrue(PointsCompressorWithQuality
                .isFloatsAlmostEquals(new BigDecimal("0.1"),new BigDecimal("0.19"), new BigDecimal("0.1")));
        assertTrue(PointsCompressorWithQuality
                .isFloatsAlmostEquals(new BigDecimal("0.1"),new BigDecimal("0.2"), new BigDecimal("0.1")));
        assertFalse(PointsCompressorWithQuality
                .isFloatsAlmostEquals(new BigDecimal("0.1"),new BigDecimal("0.21"), new BigDecimal("0.1")));
        assertTrue(PointsCompressorWithQuality
                .isFloatsAlmostEquals(new BigDecimal("1.5"),new BigDecimal("1.45"), new BigDecimal("0.05")));
        assertFalse(PointsCompressorWithQuality
                .isFloatsAlmostEquals(new BigDecimal("1.5"),new BigDecimal("1.45"), new BigDecimal("0.049")));
    }
}
