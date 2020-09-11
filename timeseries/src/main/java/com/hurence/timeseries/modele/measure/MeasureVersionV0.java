package com.hurence.timeseries.modele.measure;

import java.util.Map;

public interface MeasureVersionV0 extends Measure {
    int getYear();
    int getMonth();
    String getDay();
    Map<String,String> getTags();
}