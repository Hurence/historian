package com.hurence.forecasting;

import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ArimaParams;
import com.workday.insights.timeseries.arima.struct.ForecastResult;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import java.util.*;

public class ArimaDoubleForecaster implements Forecaster<Double> {

    private int p = 2;
    private int d = 2;
    private int q = 0;
    private int P = 0;
    private int D = 0;
    private int Q = 0;
    private int m = 0;

    @Override
    public List<Double> forecast(List<Double> inputData, int numPoints) {

        double[] dataArray = new double[inputData.size()];
        for (int i = 0; i < dataArray.length; i++) {
            dataArray[i] = inputData.get(i);
        }

        // Obtain forecast result. The structure contains forecasted values and performance metric etc.
        ForecastResult forecastResult = Arima.forecast_arima(dataArray, numPoints, new ArimaParams(p, d, q, P, D, Q, m));

        // Read forecast values
        double[] forecastData = forecastResult.getForecast();

        ArrayList<Double> forecasted = new ArrayList<Double>();
        for (int i = 0; i < forecastData.length; i++) {
            forecasted.add(forecastData[i]);
            System.out.printf("" + forecasted.get(i) + ", ");
        }
        System.out.println("");
        return forecasted;
    }


    /**
     * Find the ARIMA's parameters
     *
     * @param inputData the given elements to forecast
     * @return a list of the ARIMA's parameters
     */
    public void fit(List<Double> inputData) {
        List<Integer> order = Arrays.asList(2, 0, 2, 0, 0, 0, 0);
        p = order.get(0);
        d = order.get(1);
        q = order.get(2);
        P = order.get(3);
        D = order.get(4);
        Q = order.get(5);
        m = order.get(6);
        //TODO: find the best parameters beside input data
    }
}
