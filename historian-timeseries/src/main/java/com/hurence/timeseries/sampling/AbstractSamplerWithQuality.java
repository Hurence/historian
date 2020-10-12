package com.hurence.timeseries.sampling;

public abstract class AbstractSamplerWithQuality<SAMPLED> implements Sampler<SAMPLED> {

    protected TimeSerieHandlerWithQuality<SAMPLED> timeSerieHandlerWithQuality;
    protected int bucketSize;

    public AbstractSamplerWithQuality(TimeSerieHandlerWithQuality<SAMPLED> timeSerieHandlerWithQuality, int bucketSize) {
        this.timeSerieHandlerWithQuality = timeSerieHandlerWithQuality;
        this.bucketSize = bucketSize;
    }
}
