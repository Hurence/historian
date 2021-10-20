package com.hurence.historian.scraper.walkers;

import com.hurence.historian.scraper.types.*;
import com.hurence.timeseries.model.Measure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;

public class HistorianPrometheusMetricsWalker implements PrometheusMetricsWalker {


    private static final Logger log = LogManager.getLogger(HistorianPrometheusMetricsWalker.class);
    private final BlockingQueue<Measure> updateQueue;

    public HistorianPrometheusMetricsWalker(BlockingQueue<Measure> updateQueue) {
        this.updateQueue = updateQueue;
    }

    @Override
    public void walkStart() {
        log.debug("start walking through metrics");
    }

    @Override
    public void walkFinish(int familiesProcessed, int metricsProcessed) {
        log.debug("done walking through metrics {} families processed and {} metrics processed",
                familiesProcessed,
                metricsProcessed);
    }

    @Override
    public void walkMetricFamily(MetricFamily familyInfo, int index) {
        log.debug("walking family {}, help {}, type {} ",
                familyInfo.getName(),
                familyInfo.getHelp(),
                familyInfo.getType());
    }

    @Override
    public void walkCounterMetric(MetricFamily family, Counter metric, int index) {
        Measure measure = Measure.builder()
                .timestamp(System.currentTimeMillis())
                .name(metric.getName())
                .tags(metric.getLabels())
                .value(metric.getValue())
                .build();

        updateQueue.add(measure);
    }

    @Override
    public void walkGaugeMetric(MetricFamily family, Gauge metric, int index) {
        Measure measure = Measure.builder()
                .timestamp(System.currentTimeMillis())
                .name(metric.getName())
                .tags(metric.getLabels())
                .value(metric.getValue())
                .build();

        updateQueue.add(measure);
    }

    @Override
    public void walkSummaryMetric(MetricFamily family, Summary metric, int index) {
     /*   System.out.printf("      {\n");
        outputLabels(metric.getLabels());
        if (!metric.getQuantiles().isEmpty()) {
            System.out.printf("        \"quantiles\":{\n");
            Iterator<Quantile> iter = metric.getQuantiles().iterator();
            while (iter.hasNext()) {
                Quantile quantile = iter.next();
                System.out.printf("          \"%f\":\"%f\"%s\n",
                        quantile.getQuantile(), quantile.getValue(), (iter.hasNext()) ? "," : "");
            }
            System.out.printf("        },\n");
        }
        System.out.printf("        \"count\":\"%d\",\n", metric.getSampleCount());
        System.out.printf("        \"sum\":\"%s\"\n", Util.convertDoubleToString(metric.getSampleSum()));
        if ((index + 1) == family.getMetrics().size()) {
            System.out.printf("      }\n");
        } else {
            System.out.printf("      },\n"); // there are more coming
        }*/
    }

    @Override
    public void walkHistogramMetric(MetricFamily family, Histogram metric, int index) {
      /*  System.out.printf("      {\n");
        outputLabels(metric.getLabels());
        if (!metric.getBuckets().isEmpty()) {
            System.out.printf("        \"buckets\":{\n");
            Iterator<Bucket> iter = metric.getBuckets().iterator();
            while (iter.hasNext()) {
                Bucket bucket = iter.next();
                System.out.printf("          \"%f\":\"%d\"%s\n",
                        bucket.getUpperBound(), bucket.getCumulativeCount(), (iter.hasNext()) ? "," : "");
            }
            System.out.printf("        },\n");
        }
        System.out.printf("        \"count\":\"%d\",\n", metric.getSampleCount());
        System.out.printf("        \"sum\":\"%s\"\n", Util.convertDoubleToString(metric.getSampleSum()));
        if ((index + 1) == family.getMetrics().size()) {
            System.out.printf("      }\n");
        } else {
            System.out.printf("      },\n"); // there are more coming
        }*/
    }


}
