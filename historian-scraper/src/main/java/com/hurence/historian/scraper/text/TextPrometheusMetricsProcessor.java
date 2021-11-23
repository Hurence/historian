package com.hurence.historian.scraper.text;

import com.hurence.historian.scraper.PrometheusMetricsProcessor;
import com.hurence.historian.scraper.types.MetricFamily;
import com.hurence.historian.scraper.walkers.PrometheusMetricsWalker;

import java.io.InputStream;

/**
 * This will iterate over a list of Prometheus metrics that are given as text data.
 */
public class TextPrometheusMetricsProcessor extends PrometheusMetricsProcessor<MetricFamily> {
    public TextPrometheusMetricsProcessor(InputStream inputStream, PrometheusMetricsWalker theWalker) {
        super(inputStream, theWalker);
    }

    @Override
    public TextPrometheusMetricDataParser createPrometheusMetricDataParser() {
        return new TextPrometheusMetricDataParser(getInputStream());
    }

    @Override
    protected MetricFamily convert(MetricFamily metricFamily) {
        return metricFamily; // no conversion necessary - our text parser already uses the common api
    }

}
