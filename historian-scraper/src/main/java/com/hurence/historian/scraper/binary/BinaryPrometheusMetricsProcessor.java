package com.hurence.historian.scraper.binary;

import com.hurence.historian.scraper.PrometheusMetricsProcessor;
import com.hurence.historian.scraper.walkers.PrometheusMetricsWalker;
import io.prometheus.client.Metrics;
import io.prometheus.client.Metrics.*;


import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This will iterate over a list of Prometheus metrics that are given as binary protocol buffer data.
 */
public class BinaryPrometheusMetricsProcessor extends PrometheusMetricsProcessor<Metrics.MetricFamily> {
    public BinaryPrometheusMetricsProcessor(InputStream inputStream, PrometheusMetricsWalker theWalker) {
        super(inputStream, theWalker);
    }

    @Override
    public BinaryPrometheusMetricDataParser createPrometheusMetricDataParser() {
        return new BinaryPrometheusMetricDataParser(getInputStream());
    }

    @Override
    protected com.hurence.historian.scraper.types.MetricFamily convert(Metrics.MetricFamily family) {
        com.hurence.historian.scraper.types.MetricFamily.Builder convertedFamilyBuilder;
        com.hurence.historian.scraper.types.MetricType convertedFamilyType = com.hurence.historian.scraper.types.MetricType.valueOf(family.getType().name());

        convertedFamilyBuilder = new com.hurence.historian.scraper.types.MetricFamily.Builder();
        convertedFamilyBuilder.setName(family.getName());
        convertedFamilyBuilder.setHelp(family.getHelp());
        convertedFamilyBuilder.setType(convertedFamilyType);

        for (Metrics.Metric metric : family.getMetricList()) {
            com.hurence.historian.scraper.types.Metric.Builder<?> convertedMetricBuilder = null;
            switch (convertedFamilyType) {
                case COUNTER:
                    convertedMetricBuilder = new com.hurence.historian.scraper.types.Counter.Builder()
                            .setValue(metric.getCounter().getValue());
                    break;
                case GAUGE:
                    convertedMetricBuilder = new com.hurence.historian.scraper.types.Gauge.Builder()
                            .setValue(metric.getGauge().getValue());
                    break;
                case SUMMARY:
                    Metrics.Summary summary = metric.getSummary();
                    List<Quantile> pqList = summary.getQuantileList();
                    List<com.hurence.historian.scraper.types.Summary.Quantile> hqList;
                    hqList = new ArrayList<>(pqList.size());
                    for (Quantile pq : pqList) {
                        com.hurence.historian.scraper.types.Summary.Quantile hq;
                        hq = new com.hurence.historian.scraper.types.Summary.Quantile(
                                pq.getQuantile(), pq.getValue());
                        hqList.add(hq);
                    }
                    convertedMetricBuilder = new com.hurence.historian.scraper.types.Summary.Builder()
                            .setSampleCount(metric.getSummary().getSampleCount())
                            .setSampleSum(metric.getSummary().getSampleSum())
                            .addQuantiles(hqList);
                    break;
                case HISTOGRAM:
                    /* NO HISTOGRAM SUPPORT IN PROMETHEUS JAVA MODEL API 0.0.2. Uncomment when 0.0.3 is released
                    Histogram histo = metric.getHistogram();
                    List<Bucket> pbList = histo.getBucketList();
                    List<com.hurence.historian.scrapper.types.Histogram.Bucket> hbList;
                    hbList = new ArrayList<>(pbList.size());
                    for (Bucket pb : pbList) {
                        com.hurence.historian.scrapper.types.Histogram.Bucket hb;
                        hb = new com.hurence.historian.scrapper.types.Histogram.Bucket(pb.getUpperBound(),
                                pb.getCumulativeCount());
                        hbList.add(hb);
                    }
                    convertedMetricBuilder = new com.hurence.historian.scrapper.types.Histogram.Builder()
                            .setSampleCount(metric.getHistogram().getSampleCount())
                            .setSampleSum(metric.getHistogram().getSampleSum())
                            .addBuckets(hbList);
                    */
                    break;
            }
            convertedMetricBuilder.setName(family.getName());
            for (LabelPair labelPair : metric.getLabelList()) {
                convertedMetricBuilder.addLabel(labelPair.getName(), labelPair.getValue());
            }
            convertedFamilyBuilder.addMetric(convertedMetricBuilder.build());
        }

        return convertedFamilyBuilder.build();
    }
}
