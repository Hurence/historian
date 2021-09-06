package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;
import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ArimaParams;
import com.workday.insights.timeseries.arima.struct.ForecastResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArimaMeasureForecaster implements Forecaster<Measure> {

    private int p = 2;
    private int d = 2;
    private int q = 0;
    private int P = 0;
    private int D = 0;
    private int Q = 0;
    private int m = 0;

    @Override
    public List<Measure> forecast(List<Measure> inputData, int numPoints) {
        double[] dataArray = measureToDouble(inputData);

        // Obtain forecast result. The structure contains forecasted values and performance metric etc.
        ForecastResult forecastResult = Arima.forecast_arima(dataArray, numPoints, new ArimaParams(p, d, q, P, D, Q, m));

        // Read forecast values
        double[] forecastData = forecastResult.getForecast();

        Long lastTimestamp = inputData.get(inputData.size()-1).getTimestamp();
        Long preTimestamp = inputData.get(inputData.size()-2).getTimestamp();
        Long interval = lastTimestamp - preTimestamp;

        ArrayList<Measure> forecasted = new ArrayList<Measure>();
        for (int i = 0; i < forecastData.length; i++) {
            forecasted.add(Measure.fromValue(lastTimestamp + interval * (i+1), forecastData[i]));
        }
        return forecasted;
    }



    /**
     * Find the ARIMA's parameters
     *
     * @param trainingData the given elements to forecast
     * @return a list of the ARIMA's parameters
     */
    @Override
    public void fit(List<Measure> trainingData, List<Measure> validatingData) {
        List<Integer> order = Arrays.asList(0, 0, 0, 0, 0, 0);
        double[] train = measureToDouble(trainingData);
        double[] valid = measureToDouble(validatingData);
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
        printOrder();
    }

    public void printOrder() {
        System.out.println("p = " + p + "\td = " + d + "\tq = " + q + "\tm = " + m);
    }

    /**
     * Compute the Root Mean Squared between 2 lists of double value
     *
     * @param measured data
     * @param forecasted data
     * @return RMS
     */
    public double rootMeanSquaredError (double[] measured, double[] forecasted) {
        double mse = 0;
        for (int i=0;i<forecasted.length;i++) {
            mse += Math.pow(measured[i] - forecasted[i], 2);
        }
        return Math.sqrt(mse / forecasted.length);
    }

    /**
     * Transform a List<Measure> into a List<Double>
     *
     * @param inputData is a list of Measure
     * @return a list of double
     */
    public double[] measureToDouble(List<Measure> inputData) {
        double[] result = new double[inputData.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = inputData.get(i).getValue();
        }
        return result;
    }
}