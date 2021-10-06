package com.hurence.forecasting;


import java.io.IOException;
import java.util.List;

public interface Forecaster<T> {

    /**
     * Forecast some elements given an input collection
     *
     * @param inputData the given elements to forecast
     * @return the forecasted elements
     */
    List<T> forecast(List<T> inputData, int numPoints) throws IOException;


    /**
     * Train the forecaster model given an input collection
     *
     * @param trainingData the given elements to train the model
     */
    void fit(List<T> trainingData, List<T> validatingData) throws IOException;
}