package com.hurence.timeseries.model;

import static com.hurence.timeseries.model.Chunk.*;
import static com.hurence.timeseries.model.Chunk.MetricKey.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class MetricKeyTest {

    private static final Logger logger = LoggerFactory.getLogger(MetricKeyTest.class);

    public static Stream<Arguments> testParseThenComputeData() {
        return Stream.of(
                Arguments.of("metric0", true), // No tags
                Arguments.of("metric1" + TOKEN_SEPARATOR_CHAR +
                        "tag1" + TAG_KEY_VALUE_SEPARATOR_CHAR + "tag1Value1" + TOKEN_SEPARATOR_CHAR +
                        "tag2" + TAG_KEY_VALUE_SEPARATOR_CHAR + "tag2Value1", true),
                Arguments.of("metric1" + TOKEN_SEPARATOR_CHAR +
                        "tag1" + TAG_KEY_VALUE_SEPARATOR_CHAR + "tag1Value1", true),
                Arguments.of("metric2" + TOKEN_SEPARATOR_CHAR +
                        "a" + TAG_KEY_VALUE_SEPARATOR_CHAR + "aValue" + TOKEN_SEPARATOR_CHAR +
                        "b" + TAG_KEY_VALUE_SEPARATOR_CHAR + "bValue", true),
                Arguments.of("metric3" + TOKEN_SEPARATOR_CHAR +
                        "b" + TAG_KEY_VALUE_SEPARATOR_CHAR + "bValue" + TOKEN_SEPARATOR_CHAR +
                        "a" + TAG_KEY_VALUE_SEPARATOR_CHAR + "aValue", false), // Not alphabetically sorted tags
                Arguments.of("metric4" + TOKEN_SEPARATOR_CHAR +
                        "noTagValueSeparator", false), // Missing tag value separator
                Arguments.of("", false), // Empty metric key
                Arguments.of(null, false) // Null metric key
        );
    }

    @ParameterizedTest
    @MethodSource("testParseThenComputeData")
    public void testParseThenCompute(String id, boolean ok) {

        System.out.println("Testing metric key: [" + id + "] (should be " + (ok ? "ok" : "nok") + ")");

        MetricKey metricKey = null;
        try {
        metricKey = Chunk.MetricKey.parse(id);
        } catch(IllegalArgumentException e) {
            if (!ok) {
                // Expected failure, stop here
                System.out.println("Got expected exception: " + e.getMessage());
                return;
            }
        }
        String newMetricKeyString = metricKey.compute();

        if (ok) {
            assertEquals(id, newMetricKeyString);
        } else {
            assertNotEquals(id, newMetricKeyString);
        }

        MetricKey newMetricKey = MetricKey.parse(newMetricKeyString);
        assertEquals(metricKey, newMetricKey);
    }
}
