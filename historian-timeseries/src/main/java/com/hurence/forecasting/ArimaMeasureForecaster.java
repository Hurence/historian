package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;

import java.util.Collections;
import java.util.List;

public class ArimaMeasureForecaster implements Forecaster<Measure> {

    @Override
    public List<Measure> forecast(List<Measure> inputData, int numPoints) {

        // TODO implement ARIMA forecasting here
        return Collections.emptyList();
    }
}
