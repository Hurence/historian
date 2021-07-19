package com.hurence.timeseries.forecasting;

import com.hurence.forecasting.Forecaster;
import com.hurence.forecasting.ForecasterFactory;
import com.hurence.forecasting.ForecastingAlgorithm;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.MeasuresLoader;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ForecasterTest {

    private static final Logger logger = LoggerFactory.getLogger(ForecasterTest.class);



    private List<Measure> getInputData() {
        return Arrays.asList(
                Measure.fromValue(1L, 48d),
                Measure.fromValue(2L, 52d),
                Measure.fromValue(3L, 60d),
                Measure.fromValue(4L, 48d),
                Measure.fromValue(5L, 52d),
                Measure.fromValue(6L, 48d),
                Measure.fromValue(7L, 52d),
                Measure.fromValue(8L, 60d),
                Measure.fromValue(9L, 48d),
                Measure.fromValue(10L, 52d),
                Measure.fromValue(11L, 60d)
        );
    }


    private List<Measure> getForecasted() {
        return Arrays.asList(
                Measure.fromValue(12L, 48d),
                Measure.fromValue(13L, 52d),
                Measure.fromValue(14L, 60d)
        );
    }

    private List<Measure> getForecastedFromFile( String filePath) {
        return MeasuresLoader.loadFromCSVFile(filePath);
    }

    @Test
    public void testArimaForecasting() {
        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.ARIMA_MEASURE);



        // TODO make a working test dataset
        List<Measure> forecasted = forecaster.forecast(getInputData(), 3);

        Assertions.assertEquals(getForecasted(),forecasted);
    }


    @Test
    public void testLSTMForecasting() {

    }
}
