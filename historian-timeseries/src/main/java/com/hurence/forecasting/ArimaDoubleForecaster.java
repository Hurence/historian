package com.hurence.forecasting;

import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ArimaParams;
import com.workday.insights.timeseries.arima.struct.ForecastResult;

import java.io.IOException;
import java.util.*;

public class ArimaDoubleForecaster implements Forecaster<Double> {

    private int p = 2;
    private int d = 0;
    private int q = 2;
    private int P = 0;
    private int D = 0;
    private int Q = 0;
    private int m = 0;

    /**
     * Forecast the number of points we want of a time series
     *
     * @param inputData the series which we want to predict the next points
     * @param numPoints number of points to forecast
     * @return a list of double forecasted values
     * @throws IOException
     */
    @Override
    public List<Double> forecast(List<Double> inputData, int numPoints) {

        double[] dataArray = new double[inputData.size()];
        for (int i = 0; i < dataArray.length; i++) {
            dataArray[i] = inputData.get(i);
        }
        verifier();
        ForecastResult forecastResult = Arima.forecast_arima(dataArray, numPoints, new ArimaParams(p, d, q, P, D, Q, m));
        double[] forecastData = forecastResult.getForecast();

        ArrayList<Double> forecasted = new ArrayList<Double>();
        for (double forecastDatum : forecastData) {
            forecasted.add(forecastDatum);
        }
        return forecasted;
    }


    /**
     * Find the ARIMA's parameters
     *
     * @param trainingData the given elements to train the model
     * @param validatingData the elements to compare with the forecasted values
     */
    @Override
    public void fit(List<Double> trainingData, List<Double> validatingData) {
        List<Integer> order = Arrays.asList(0, 0, 0, 0, 0, 0);
        double[] train = listToDouble(trainingData);
        double[] valid = listToDouble(validatingData);
        double minMse = 99999999;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 10; k++) {
                    ForecastResult forecastResult = null;
                    if (i == 0 && k == 0) {
                        forecastResult = Arima.forecast_arima(train, valid.length, new ArimaParams(1, j, k, P, D, Q, 1+k+1));
                    } else {
                        forecastResult = Arima.forecast_arima(train, valid.length, new ArimaParams(i, j, k, P, D, Q, i+k+1));
                    }
                    double[] forecastData = forecastResult.getForecast();
                    double mse = rootMeanSquaredError(valid, forecastData);
                    if (mse < minMse) {
                        minMse = mse;
                        order.set(0, i);
                        order.set(1, j);
                        order.set(2, k);
                    }
                }
            }
        }
        p = order.get(0);
        d = order.get(1);
        q = order.get(2);
        P = order.get(3);
        D = order.get(4);
        Q = order.get(5);
        m = p + q + P + Q + 1;
    }

    /**
     * Print the ARIMA's parameters
     */
    public void printOrder() {
        System.out.println("p = " + p + "\td = " + d + "\tq = " + q + "\tm = " + m);
    }

    /**
     * Compute the root mean squared error of two lists of doubles
     *
     * @param measured data
     * @param forecasted data
     * @return root mean squared error
     */
    public double rootMeanSquaredError (double[] measured, double[] forecasted) {
        double mse = 0;
        for (int i=0;i<forecasted.length;i++) {
            mse += Math.pow(measured[i] - forecasted[i], 2);
        }
        return Math.sqrt(mse / forecasted.length);
    }

    /**
     * Transform a List<Double> into a double[]
     *
     * @param inputData is a list of Measure
     * @return a list of double
     */
    public double[] listToDouble(List<Double> inputData) {
        double[] result = new double[inputData.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = inputData.get(i);
        }
        return result;
    }

    /**
     * Verification of ARIMA's parameters (p and q must not = 0 at same time)
     */
    public void verifier() {
        if (p == 0 && q == 0) {
            p=1;
        }
    }
}
