package com.hurence.timeseries.analysis;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;


import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class TimeseriesAnalyzerTest {

    @Test
    public void testMixedNaNValues() {

        DateTime time = new DateTime(1977,3,2,2,13)
                .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC"))));

        List<Long> timestamps = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        for(int i=0; i<100; i++){
            long newTime = time.getMillis() + (long) (Math.random() * 1000L);
            double newValue = Math.random()* 100;
            values.add(  newValue < 10 ? Double.NaN : newValue);
            timestamps.add( newTime );
        }


        TimeseriesAnalysis analysis = TimeseriesAnalyzer.builder()
                .build()
                .run(timestamps, values);

        Assertions.assertFalse(Double.isNaN(analysis.getMean()));
    }
}
