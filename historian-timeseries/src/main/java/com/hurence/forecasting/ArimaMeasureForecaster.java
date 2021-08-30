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

    @Override
    public List<Measure> forecast(List<Measure> inputData, int numPoints) {
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


    public List<Integer> autoArima(List<Measure> inputData) {
        //TODO: find the best parameters beside input data
        
        return Arrays.asList(2, 0, 2, 0, 0, 0, 0);
    }
}