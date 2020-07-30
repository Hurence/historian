package com.hurence.timeseries.sampling;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.List;
import java.util.stream.Collectors;

public class MinSamplerWithQuality<SAMPLED> extends AbstractSamplerWithQuality<SAMPLED> {

    public MinSamplerWithQuality(TimeSerieHandlerWithQuality<SAMPLED> timeSerieHandlerWithQuality, int bucketSize) {
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
        final int realBucketSize = SamplingUtils.fitBucketSize(inputRecords, bucketSize);
        return SamplingUtils.grouped(inputRecords, realBucketSize)
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
                    final double minValue = stats.getMin();
                    final float minQuality = (float) statsQuality.getMin();
                    final long timestamp = timeSerieHandlerWithQuality.getTimeserieTimestamp(bucket.get(0));
                    final SAMPLED sampleElement = timeSerieHandlerWithQuality.createTimeserie(timestamp, minValue, minQuality);
                    return sampleElement;
                }).collect(Collectors.toList());
    }
}