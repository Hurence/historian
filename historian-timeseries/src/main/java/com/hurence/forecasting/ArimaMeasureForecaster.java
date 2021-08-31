package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;
import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ArimaParams;
import com.workday.insights.timeseries.arima.struct.ForecastResult;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

        double[] dataArray = new double[inputData.size()];
        for (int i = 0; i < dataArray.length; i++) {
            dataArray[i] = inputData.get(i).getValue();
        }

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
            System.out.printf(""+forecasted.get(i).getValue()+ ", ");
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
    @Override
    public void fit(List<Measure> inputData) {
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