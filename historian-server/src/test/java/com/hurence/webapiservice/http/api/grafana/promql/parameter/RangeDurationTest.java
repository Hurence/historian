package com.hurence.webapiservice.http.api.grafana.promql.parameter;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.hurence.historian.model.HistorianServiceFields.MATCH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RangeDurationTest {

    @Test
    void testMatch() {

        String query = "increase(node_cpu_seconds_total[1h])";
        RangeDuration rangeDuration = RangeDuration.builder().parse(query).build();

        assertEquals(Duration.ofHours(1),rangeDuration.getDuration());

        String query2 = "increase(node_cpu_seconds_total[36m])";
        RangeDuration rangeDuration2 = RangeDuration.builder().parse(query2).build();

        assertEquals(Duration.ofMinutes(36),rangeDuration2.getDuration());

        String query3 = "increase(node_cpu_seconds_total[4d])";
        RangeDuration rangeDuration3 = RangeDuration.builder().parse(query3).build();

        assertEquals(Duration.ofDays(4),rangeDuration3.getDuration());

/*
        String query4 = "increase(node_cpu_seconds_total[4w])";
        RangeDuration rangeDuration4 = RangeDuration.builder().parse(query3).build();

        assertEquals(Duration.ofDays(7*4),rangeDuration4.getDuration());*/
    }

    @Test
    void testReplacement() {
        String query = "node_cpu_seconds_total[4d]";
        RangeDuration rangeDuration = RangeDuration.builder().parse(query).build();

        assertEquals("node_cpu_seconds_total",rangeDuration.getQueryWithoutDuration());

        String query3 = "increase(node_cpu_seconds_total[4d])";
        RangeDuration rangeDuration3 = RangeDuration.builder().parse(query3).build();

        assertEquals("increase(node_cpu_seconds_total)",rangeDuration3.getQueryWithoutDuration());

        String query4 = "increase(node_cpu_seconds_total{a=\"aze\"}[4d])";
        RangeDuration rangeDuration4 = RangeDuration.builder().parse(query4).build();

        assertEquals("increase(node_cpu_seconds_total{a=\"aze\"})",rangeDuration4.getQueryWithoutDuration());
    }



    }