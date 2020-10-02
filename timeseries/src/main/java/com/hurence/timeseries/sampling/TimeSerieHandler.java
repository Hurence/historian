package com.hurence.timeseries.sampling;

public interface TimeSerieHandler<TIMESERIE> {

    TIMESERIE createTimeserie(long timestamp, double value);

    long getTimeserieTimestamp(TIMESERIE timeserie);

    Double getTimeserieValue(TIMESERIE timeserie);
}
