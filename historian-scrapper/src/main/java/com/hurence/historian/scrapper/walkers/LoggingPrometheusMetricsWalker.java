package com.hurence.historian.scrapper.walkers;




import com.hurence.historian.scrapper.types.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Map;

/**
 * This implementation simply logs the metric values.
 */
public class LoggingPrometheusMetricsWalker implements PrometheusMetricsWalker {
    private static final Logger log = LogManager.getLogger(LoggingPrometheusMetricsWalker.class);
    private Level logLevel;

    public LoggingPrometheusMetricsWalker() {
        this(null);
    }

    public LoggingPrometheusMetricsWalker(Level logLevel) {
        this.logLevel = (logLevel != null) ? logLevel : Level.DEBUG;
    }

    @Override
    public void walkStart() {
    }

    @Override
    public void walkFinish(int familiesProcessed, int metricsProcessed) {
    }

    @Override
    public void walkMetricFamily(MetricFamily family, int index) {
        log.info("Metric Family [{}] of type [{}] has [{}] metrics: {}",
                family.getName(),
                family.getType(),
                family.getMetrics().size(),
                family.getHelp());
    }

    @Override
    public void walkCounterMetric(MetricFamily family, Counter metric, int index) {
        log.info("COUNTER: {}{}={}",
                metric.getName(),
                buildLabelListString(metric.getLabels()),
                metric.getValue());
    }

    @Override
    public void walkGaugeMetric(MetricFamily family, Gauge metric, int index) {
        log.info("GAUGE: {}{}={}",
                metric.getName(),
                buildLabelListString(metric.getLabels()),
                metric.getValue());
    }

    @Override
    public void walkSummaryMetric(MetricFamily family, Summary metric, int index) {
        log.info("SUMMARY: {}{}: count={}, sum={}, quantiles={}",
                metric.getName(),
                buildLabelListString(metric.getLabels()),
                metric.getSampleCount(),
                metric.getSampleSum(),
                metric.getQuantiles());
    }

    @Override
    public void walkHistogramMetric(MetricFamily family, Histogram metric, int index) {
        log.info("HISTOGRAM: {}{}: count={}, sum={}, buckets={}",
                metric.getName(),
                buildLabelListString(metric.getLabels()),
                metric.getSampleCount(),
                metric.getSampleSum(),
                metric.getBuckets());
    }

    /**
     * The default implementations of the walk methods will log the metric data with this given log level.
     *
     * @return the log level
     */
    protected Level getLogLevel() {
        return this.logLevel;
    }

    protected String buildLabelListString(Map<String, String> labels) {
        return buildLabelListString(labels, "{", "}");
    }
}
