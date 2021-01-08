package com.hurence.timeseries.sampling;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.List;
import java.util.stream.Collectors;

public class MaxSamplerWithQuality<SAMPLED> extends AbstractSamplerWithQuality<SAMPLED> {

    public MaxSamplerWithQuality(TimeSerieHandlerWithQuality<SAMPLED> timeSerieHandlerWithQuality, int bucketSize) {
        super(timeSerieHandlerWithQuality, bucketSize);
    }

    /**
     * divide the points sequence into equally sized buckets
     * and compute average of each bucket
     *
     * @param inputRecords the input list
     * @return
     */
    @Override
    public List<SAMPLED> sample(List<SAMPLED> inputRecords) {
        return SamplingUtils.grouped(inputRecords, bucketSize)
                .map(bucket -> {
                    SummaryStatistics stats = new SummaryStatistics();
                    bucket.forEach(element -> {
                        final Double recordValue = timeSerieHandlerWithQuality.getTimeserieValue(element);
                        if (recordValue != null)
                            stats.addValue(recordValue);
                    });
                    SummaryStatistics statsQuality = new SummaryStatistics();
                    bucket.forEach(element -> {
                        final Float recordQuality = timeSerieHandlerWithQuality.getTimeserieQuality(element);
                        if (recordQuality != null)
                            statsQuality.addValue(recordQuality);
                    });
                    final double maxValue = stats.getMax();
                    final float maxQuality = (float) statsQuality.getMax();
                    final long timestamp = timeSerieHandlerWithQuality.getTimeserieTimestamp(bucket.get(0));
                    final SAMPLED sampleElement = timeSerieHandlerWithQuality.createTimeserie(timestamp, maxValue, maxQuality);
                    return sampleElement;
                }).collect(Collectors.toList());
    }
}