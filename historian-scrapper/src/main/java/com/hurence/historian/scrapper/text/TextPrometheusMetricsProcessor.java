package com.hurence.historian.scrapper.text;

import com.hurence.historian.scrapper.PrometheusMetricsProcessor;
import com.hurence.historian.scrapper.types.MetricFamily;
import com.hurence.historian.scrapper.walkers.PrometheusMetricsWalker;

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
