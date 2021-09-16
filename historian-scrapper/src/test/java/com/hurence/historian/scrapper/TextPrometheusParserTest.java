/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.historian.scrapper;


import static org.junit.jupiter.api.Assertions.*;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import com.hurence.historian.scrapper.text.TextPrometheusMetricDataParser;
import com.hurence.historian.scrapper.text.TextPrometheusMetricsProcessor;
import com.hurence.historian.scrapper.types.*;
import com.hurence.historian.scrapper.walkers.LoggingPrometheusMetricsWalker;
import com.hurence.historian.scrapper.walkers.PrometheusMetricsWalker;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TextPrometheusParserTest {

    private List<MetricFamily> parseTestFile(String fileName) throws Exception {
        List<MetricFamily> metricFamilies = new ArrayList<>();

        try (InputStream testData = getClass().getClassLoader().getResourceAsStream(fileName)) {
            TextPrometheusMetricDataParser parser = new TextPrometheusMetricDataParser(testData);
            while (true) {
                MetricFamily family = parser.parse();
                if (family == null) {
                    break;
                }
                metricFamilies.add(family);
            }
        }

        return metricFamilies;
    }

    @Test
    public void testCounter() throws Exception {
        List<MetricFamily> metricFamilies = parseTestFile("prometheus-counter.txt");

        assertNotNull(metricFamilies);
        assertEquals(1, metricFamilies.size());
        assertEquals("http_requests_total", metricFamilies.get(0).getName());
        assertEquals("Total number of HTTP requests made.", metricFamilies.get(0).getHelp());
        assertEquals(MetricType.COUNTER, metricFamilies.get(0).getType());
        assertEquals(5, metricFamilies.get(0).getMetrics().size());

        Counter counter = (Counter) metricFamilies.get(0).getMetrics().get(0);
        assertEquals("http_requests_total", counter.getName());
        assertEquals(162030, counter.getValue(), 0.1);
        assertEquals("200", counter.getLabels().get("code"));
        assertEquals("prometheus", counter.getLabels().get("handler"));
        assertEquals("get", counter.getLabels().get("method"));
    }

    @Test
    public void testGauge() throws Exception {
        List<MetricFamily> metricFamilies = parseTestFile("prometheus-gauge.txt");

        assertNotNull(metricFamilies);
        assertEquals(1, metricFamilies.size());
        assertEquals("go_memstats_alloc_bytes", metricFamilies.get(0).getName());
        assertEquals("", metricFamilies.get(0).getHelp());
        assertEquals(MetricType.GAUGE, metricFamilies.get(0).getType());
        assertEquals(1, metricFamilies.get(0).getMetrics().size());

        Gauge gauge = (Gauge) metricFamilies.get(0).getMetrics().get(0);
        assertEquals("go_memstats_alloc_bytes", gauge.getName());
        assertEquals(4.14422136e+08, gauge.getValue(), 0.1);
        assertEquals(0, gauge.getLabels().size());

    }

    @Test
    public void testSummary() throws Exception {
        List<MetricFamily> metricFamilies = parseTestFile("prometheus-summary.txt");

        assertNotNull(metricFamilies);
        assertEquals(1, metricFamilies.size());
        assertEquals("prometheus_local_storage_maintain_series_duration_milliseconds",
                metricFamilies.get(0).getName());
        assertEquals("The duration (in milliseconds) it took to perform maintenance on a series.",
                metricFamilies.get(0).getHelp());
        assertEquals(MetricType.SUMMARY, metricFamilies.get(0).getType());
        assertEquals(2, metricFamilies.get(0).getMetrics().size());

        // the metrics should appear in order as they appear in the text. First is "memory", second is "disk"
        Summary first = (Summary) metricFamilies.get(0).getMetrics().get(0);
        Summary second = (Summary) metricFamilies.get(0).getMetrics().get(1);
        assertEquals(1, first.getLabels().size());
        assertTrue(first.getLabels().containsKey("location"));
        assertEquals("memory", first.getLabels().get("location"));
        assertEquals(1, second.getLabels().size());
        assertTrue(second.getLabels().containsKey("location"));
        assertEquals("disk", second.getLabels().get("location"));
        assertEquals(12345, second.getSampleCount());
        assertEquals(1.5, second.getSampleSum(), 0.01);
        assertEquals(0.50, second.getQuantiles().get(0).getQuantile(), 0.01);
        assertEquals(40.0, second.getQuantiles().get(0).getValue(), 0.01);
        assertEquals(0.90, second.getQuantiles().get(1).getQuantile(), 0.01);
        assertEquals(50.0, second.getQuantiles().get(1).getValue(), 0.01);
        assertEquals(0.99, second.getQuantiles().get(2).getQuantile(), 0.01);
        assertEquals(60.0, second.getQuantiles().get(2).getValue(), 0.01);

    }

    @Test
    public void testHistogram() throws Exception {
        List<MetricFamily> metricFamilies = parseTestFile("prometheus-histogram.txt");

        assertNotNull(metricFamilies);
        assertEquals(1, metricFamilies.size());
        assertEquals("http_request_duration_seconds", metricFamilies.get(0).getName());
        assertEquals("A histogram of the request duration.", metricFamilies.get(0).getHelp());
        assertEquals(MetricType.HISTOGRAM, metricFamilies.get(0).getType());
        assertEquals(1, metricFamilies.get(0).getMetrics().size());
        Histogram metric = (Histogram) metricFamilies.get(0).getMetrics().get(0);
        assertEquals(1, metric.getLabels().size());
        assertTrue(metric.getLabels().containsKey("mylabel"));
        assertEquals("wotgorilla?", metric.getLabels().get("mylabel"));
        assertEquals(144320, metric.getSampleCount());
        assertEquals(53423, metric.getSampleSum(), 0.01);
        assertEquals(0.05, metric.getBuckets().get(0).getUpperBound(), 0.001);
        assertEquals(24054, metric.getBuckets().get(0).getCumulativeCount());
        assertEquals(0.1, metric.getBuckets().get(1).getUpperBound(), 0.001);
        assertEquals(33444, metric.getBuckets().get(1).getCumulativeCount());
        assertEquals(0.2, metric.getBuckets().get(2).getUpperBound(), 0.001);
        assertEquals(100392, metric.getBuckets().get(2).getCumulativeCount());
        assertEquals(0.5, metric.getBuckets().get(3).getUpperBound(), 0.001);
        assertEquals(129389, metric.getBuckets().get(3).getCumulativeCount());
        assertEquals(1.0, metric.getBuckets().get(4).getUpperBound(), 0.001);
        assertEquals(133988, metric.getBuckets().get(4).getCumulativeCount());
        assertEquals(Double.POSITIVE_INFINITY, metric.getBuckets().get(5).getUpperBound(), 0.001);
        assertEquals(144320, metric.getBuckets().get(5).getCumulativeCount());
    }

    @Test
    public void testThreeCounters() throws Exception {
        List<MetricFamily> metricFamilies = parseTestFile("prometheus-three-counters.txt");

        assertNotNull(metricFamilies);
        assertEquals(3, metricFamilies.size());

        // the first one

        int familyIndex = 0;
        assertEquals("one_counter_total", metricFamilies.get(familyIndex).getName());
        assertEquals("This is the first", metricFamilies.get(familyIndex).getHelp());
        assertEquals(MetricType.COUNTER, metricFamilies.get(familyIndex).getType());
        assertEquals(1, metricFamilies.get(familyIndex).getMetrics().size());

        Counter counter = (Counter) metricFamilies.get(familyIndex).getMetrics().get(0);
        assertEquals("one_counter_total", counter.getName());
        assertEquals(111, counter.getValue(), 0.1);
        assertEquals(0, counter.getLabels().size());

        // the second one - tests that the text data parser can properly buffer lines between families

        familyIndex = 1;
        assertEquals("two_counter_total", metricFamilies.get(familyIndex).getName());
        assertEquals("This is the second", metricFamilies.get(familyIndex).getHelp());
        assertEquals(MetricType.COUNTER, metricFamilies.get(familyIndex).getType());
        assertEquals(1, metricFamilies.get(familyIndex).getMetrics().size());

        counter = (Counter) metricFamilies.get(familyIndex).getMetrics().get(0);
        assertEquals("two_counter_total", counter.getName());
        assertEquals(222, counter.getValue(), 0.1);
        assertEquals(0, counter.getLabels().size());

        // the third one

        familyIndex = 2;
        assertEquals("three_counter_total", metricFamilies.get(familyIndex).getName());
        assertEquals("This is the third with type specified first", metricFamilies.get(familyIndex).getHelp());
        assertEquals(MetricType.COUNTER, metricFamilies.get(familyIndex).getType());
        assertEquals(1, metricFamilies.get(familyIndex).getMetrics().size());

        counter = (Counter) metricFamilies.get(familyIndex).getMetrics().get(0);
        assertEquals("three_counter_total", counter.getName());
        assertEquals(333, counter.getValue(), 0.1);
        assertEquals(0, counter.getLabels().size());
    }

    @Test
    public void testGetMetricsFromStream() throws Exception {

        List<MetricFamily> metricFamilies = parseTestFile("prometheus.txt");

        assertNotNull(metricFamilies);
        assertEquals(72, metricFamilies.size());

        // walk the data and make sure it has what we expect
        final AtomicInteger familyCount = new AtomicInteger(0);
        final AtomicInteger fullCount = new AtomicInteger(0);
        PrometheusMetricsWalker walker = new LoggingPrometheusMetricsWalker(Level.INFO) {
            public void walkMetricFamily(MetricFamily family, int index) {
                super.walkMetricFamily(family, index);
                familyCount.incrementAndGet();
                fullCount.addAndGet(family.getMetrics().size());
            }
        };

        try (InputStream testData = this.getClass().getClassLoader().getResourceAsStream("prometheus.txt")) {
            new TextPrometheusMetricsProcessor(testData, walker).walk();
        }
        assertEquals(metricFamilies.size(), familyCount.get());
        assertEquals(127, fullCount.get());
    }

    @Test
    public void testGetMetricsFromUrl() throws Exception {
        URL testDataUrl = getClass().getClassLoader().getResource("prometheus.txt");
        PrometheusScraper scraper = new PrometheusScraper(testDataUrl, PrometheusDataFormat.TEXT);

        // walk the data and make sure it has what we expect
        final AtomicInteger familyCount = new AtomicInteger(0);
        final AtomicInteger fullCount = new AtomicInteger(0);
        PrometheusMetricsWalker walker = new LoggingPrometheusMetricsWalker(Level.INFO) {
            public void walkMetricFamily(MetricFamily family, int index) {
                super.walkMetricFamily(family, index);
                familyCount.incrementAndGet();
                fullCount.addAndGet(family.getMetrics().size());
            }
        };
        scraper.scrape(walker);

        assertEquals(72, familyCount.get());
        assertEquals(127, fullCount.get());

        // test the scrape() method
        scraper = new PrometheusScraper(testDataUrl, PrometheusDataFormat.TEXT);
        List<MetricFamily> allFamilies = scraper.scrape();
        assertEquals(72, allFamilies.size());
    }
}
