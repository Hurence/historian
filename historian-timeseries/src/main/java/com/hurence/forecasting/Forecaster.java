package com.hurence.forecasting;

import java.util.List;

public interface Forecaster<T> {

    /**
     * Forecast some elements guiven an input collection
     *
     * @param inputData the given elements to forecast
     * @return the forecasted elements
     */
    List<T> forecast(List<T> inputData, int numPoints);
}