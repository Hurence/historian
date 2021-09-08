package com.hurence.timeseries.forecasting;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.*;
import com.hurence.forecasting.Forecaster;
import com.hurence.forecasting.ForecasterFactory;
import com.hurence.forecasting.ForecastingAlgorithm;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.MeasuresLoader;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ForecasterTest {
    private static final int numPoints = 3;
    private static final int limit = 10;
    private static final String file = "test_data0.csv";

    private static final Logger logger = LoggerFactory.getLogger(ForecasterTest.class);


    private void affichage() {
        System.out.println("\n---------------------------------------------------------------------------------------\n" +
                "      Forecasting Tests :    \n" +
                "---------------------------------------------------------------------------------------");
    }

    private void printd(List<Double> inputs) {
        System.out.print("[");
        for (Double elmt : inputs) {
            System.out.printf("%.2f", elmt);
            System.out.print(", ");
        }
        System.out.println("]");
    }
    private void printm(List<Measure> inputs) {
        System.out.print("[");
        for (Measure elmt : inputs) {
            System.out.printf("%.2f", elmt.getValue());
            System.out.print(", ");
        }
        System.out.println("]");
    }
    private void printt(List<Measure> inputs) {
        System.out.print("[");
        for (Measure elmt : inputs) {
            System.out.printf("" + elmt.getTimestamp() + ", ");
        }
        System.out.println("]");
    }

    /**
     * Collect data from resources/data directory
     *
     * @return list of Measures with their timestamp and their value
     * @throws IOException
     */
    private List<Measure> getInputMeasureData(String file) throws IOException {
        return getDataFromFile("src/test/resources/data/"+ file);
    }

    /**
     * Collect data from resources/data directory
     *
     * @return list of values type=Double
     * @throws IOException
     */
    private List<Double> getInputDoubleData(String file) throws IOException {
        // TODO : lecture d'un fichier csv
        File csvFile = new File("src/test/resources/data/"+ file).getAbsoluteFile();

        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        MappingIterator<List<String>> rows = csvMapper.readerFor(List.class).readValues(csvFile);
        ArrayList<Double> inputs = new ArrayList<Double>();

        for (MappingIterator<List<String>> it = rows; it.hasNext(); ) {
            List<String> row = it.next();
            if (!row.get(0).equals("timestamp")) {
                Double value = Double.parseDouble(row.get(1));
                inputs.add(value);
            }
        }
        return inputs;
    }

    /**
     * Call loadFromCSVFile method from MeasuresLoader class to load the data from a CSV file
     * and store them in a list of Measure
     *
     * @param filePath of the CSV file
     * @return the list of Measure with timestamp and value tag
     * @throws IOException
     */
    private List<Measure> getDataFromFile(String filePath) throws IOException {
        return MeasuresLoader.loadFromCSVFile(filePath);
    }

    /**
     * Compute the Root Mean Squared between 2 lists of double value
     *
     * @param measured data
     * @param forecasted data
     * @return RMS
     */
    private double rootMeanSquaredErrorDouble (List<Double> measured, List<Double> forecasted) {
        double mse = 0;
        for (int i=0;i<forecasted.size();i++) {
            mse += Math.pow(measured.get(i) - forecasted.get(i), 2);
        }
        return Math.sqrt(mse / forecasted.size());
    }

    /**
     * Compute the Root Mean Squared between 2 lists of Measure value
     *
     * @param measured data
     * @param forecasted data
     * @return RMS
     */
    private double rootMeanSquaredErrorMeasure (List<Measure> measured, List<Measure> forecasted) {
        double mse = 0;
        for (int i=0;i<forecasted.size();i++) {
            mse += Math.pow(measured.get(i).getValue() - forecasted.get(i).getValue(), 2);
        }
        return Math.sqrt(mse / forecasted.size());
    }

    @Test
    public void testArimaDoubleForecasting() throws IOException {
        System.out.println("\nARIMA_DOUBLE TEST :");
        Forecaster<Double> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.ARIMA_DOUBLE);

        List<Double> inputs = getInputDoubleData(file);
        List<Double> training = inputs.subList(0, inputs.size() - numPoints);
        List<Double> validating = inputs.subList(inputs.size()-numPoints, inputs.size());

        double debut = System.currentTimeMillis();
        forecaster.fit(training, validating);
        double fin = System.currentTimeMillis();
        double time = (fin - debut)/1000;

        debut = System.currentTimeMillis();
        List<Double> forecasted = forecaster.forecast(training, numPoints);
        fin = System.currentTimeMillis();
        double iTime = (fin - debut)/1000;

        if (numPoints < 20) {
            printd(validating);
            printd(forecasted);
        }

        Double mse = rootMeanSquaredErrorDouble(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);
        System.out.println("Fitting time : " + time + "s");
        System.out.println("Inference time : " + iTime + "s");

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);
    }

    @Test
    public void testArimaMeasureForecasting() throws IOException {
        System.out.println("\nARIMA_MEASURE TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.ARIMA_MEASURE);

        List<Measure> inputs = getInputMeasureData(file);
        List<Measure> training = inputs.subList(0, inputs.size() - numPoints);
        List<Measure> validating = inputs.subList(inputs.size()-numPoints, inputs.size());

        double debut = System.currentTimeMillis();
        forecaster.fit(training, validating);
        double fin = System.currentTimeMillis();
        double fTime = (fin - debut)/1000;

        debut = System.currentTimeMillis();
        List<Measure> forecasted = forecaster.forecast(training, numPoints);
        fin = System.currentTimeMillis();
        double iTime = (fin - debut)/1000;

        if (numPoints < 20) {
            printm(validating);
            printm(forecasted);
        }

        Double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);
        System.out.println("Fitting time : " + fTime + "s");
        System.out.println("Inference time : " + iTime + "s");

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);
    }

    @Test
    public void testLSTMForecasting() throws IOException {
        System.out.println("\nLSTM TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.LSTM);

        int lookback = Math.min(numPoints, 30);

        List<Measure> inputs = getInputMeasureData(file);
        List<Measure> training = inputs.subList(0, inputs.size() - numPoints);
        List<Measure> validating = inputs.subList(inputs.size() - numPoints - lookback, inputs.size());

        double debut = System.currentTimeMillis();
        forecaster.fit(training, validating);
        double fin = System.currentTimeMillis();
        double fTime = (fin - debut)/1000;

        debut = System.currentTimeMillis();
//        List<Measure> forecasted = forecaster.forecast(inputs.subList(inputs.size() - numPoints - 2, inputs.size()), numPoints);
        List<Measure> forecasted = forecaster.forecast(validating, numPoints);
        fin = System.currentTimeMillis();
        double iTime = (fin - debut)/1000;

        if (numPoints < 20) {
            printm(inputs.subList(inputs.size()-numPoints, inputs.size()));
            printm(forecasted);
        }

        double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);
        System.out.println("Fitting time : " + fTime + "s");
        System.out.println("Inference time : " + iTime + "s");

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);

    }

    @Test
    public void testDNNForecasting() throws IOException {
        affichage();
        System.out.println("\nDNN TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.DNN);

        List<Measure> inputs = getInputMeasureData(file);
        List<Measure> training = inputs.subList(0, inputs.size() - numPoints);
        List<Measure> validating = inputs.subList(inputs.size() - numPoints, inputs.size());

        double debut = System.currentTimeMillis();
        forecaster.fit(training, validating);
        double fin = System.currentTimeMillis();
        double fTime = (fin - debut)/1000;

        debut = System.currentTimeMillis();
        List<Measure> forecasted = forecaster.forecast(validating, numPoints);
        fin = System.currentTimeMillis();
        double iTime = (fin - debut)/1000;

        if (numPoints < 20) {
            printm(validating);
            printm(forecasted);
        }

        double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);
        System.out.println("Fitting time : " + fTime + "s");
        System.out.println("Inference time : " + iTime + "s");

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);

    }
}
