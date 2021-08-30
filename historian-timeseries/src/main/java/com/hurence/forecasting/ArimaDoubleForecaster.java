package com.hurence.forecasting;

import com.workday.insights.timeseries.arima.Arima;
import com.workday.insights.timeseries.arima.struct.ArimaParams;
import com.workday.insights.timeseries.arima.struct.ForecastResult;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import java.util.*;

public class ArimaDoubleForecaster implements Forecaster<Double> {


    @Override
    public List<Double> forecast(List<Double> inputData, int numPoints) {
        List<Integer> order = autoArima(inputData);
        int p = order.get(0);
        int d = order.get(1);
        int q = order.get(2);
        int P = order.get(3);
        int D = order.get(4);
        int Q = order.get(5);
        int m = order.get(6);

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
            System.out.printf(""+forecasted.get(i)+ ", ");
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
    public List<Integer> autoArima(List<Double> inputData) {
         // Verify if inputData is constant
//        if (new HashSet<Double>(inputData).size() <= 1) {
//            return Arrays.asList(0, 0, 0, 0, 0, 0, 0);
//        }
        //TODO: find the best parameters beside input data
        return Arrays.asList(2, 0, 2, 0, 0, 0, 0);
    }
}
