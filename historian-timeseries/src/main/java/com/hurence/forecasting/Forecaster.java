package com.hurence.forecasting;

import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import java.util.List;

public interface Forecaster<T> {

    /**
     * Forecast some elements given an input collection
     *
     * @param inputData the given elements to forecast
     * @return the forecasted elements
     */
    List<T> forecast(List<T> inputData, int numPoints);
}