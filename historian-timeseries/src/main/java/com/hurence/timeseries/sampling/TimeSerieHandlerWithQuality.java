package com.hurence.timeseries.sampling;

public interface TimeSerieHandlerWithQuality<TIMESERIE> {

    TIMESERIE createTimeserie(long timestamp, double value, float quality);

    long getTimeserieTimestamp(TIMESERIE timeserie);

    Double getTimeserieValue(TIMESERIE timeserie);

    Float getTimeserieQuality(TIMESERIE timeserie);
}
