package com.hurence.timeseries.forecasting;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.*;
import com.hurence.forecasting.Forecaster;
import com.hurence.forecasting.ForecasterFactory;
import com.hurence.forecasting.ForecastingAlgorithm;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.MeasuresLoader;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.jfree.data.general.Dataset;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ForecasterTest {
    private static int numPoints = 3;
    private static int limit = 5;

    private static final Logger logger = LoggerFactory.getLogger(ForecasterTest.class);


    private void affichage() {
        System.out.println("\n---------------------------------------------------------------------------------------\n" +
                "      Forecasting Tests :    \n" +
                "---------------------------------------------------------------------------------------");
    }

    private void printd(List<Double> inputs) {
        System.out.printf("[");
        for (Double elmt : inputs) {
            System.out.printf("" + elmt + ", ");
        }
        System.out.println("]");
    }
    
    private void printm(List<Measure> inputs) {
        System.out.printf("[");
        for (Measure elmt : inputs) {
            System.out.printf("" + elmt.getValue() + ", ");
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

        List<Double> inputs = getInputDoubleData("test_data.csv");
        List<Double> training = inputs.subList(0, inputs.size() - numPoints);
        List<Double> validating = inputs.subList(inputs.size()-numPoints, inputs.size());

        printd(validating);

        List<Double> forecasted = forecaster.forecast(training, numPoints);
        Double mse = rootMeanSquaredErrorDouble(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);
    }

    @Test
    public void testArimaMeasureForecasting() throws IOException {
        System.out.println("\nARIMA_MEASURE TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.ARIMA_MEASURE);

        List<Measure> inputs = getInputMeasureData("test_data.csv");
        List<Measure> training = inputs.subList(0, inputs.size() - numPoints);
        List<Measure> validating = inputs.subList(inputs.size()-numPoints, inputs.size());

        printm(validating);

        List<Measure> forecasted = forecaster.forecast(training, numPoints);

        Double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);
    }

    @Test
    public void testLSTMForecasting() throws IOException {
        System.out.println("\nLSTM TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.LSTM);


        List<Measure> inputs = getInputMeasureData("test_data.csv");

        List<Measure> forecasted = forecaster.forecast(inputs, numPoints);

    }

    @Test
    public void testDNNForecasting() throws IOException {
        affichage();
        System.out.println("\nDNN TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.DNN);


        List<Measure> inputs = getInputMeasureData("test_data.csv");

        List<Measure> forecasted = forecaster.forecast(inputs, numPoints);

    }
}
