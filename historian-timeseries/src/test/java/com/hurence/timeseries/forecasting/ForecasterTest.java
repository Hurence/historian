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

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ForecasterTest {
    public static int numPoints = 400;
    private static final int limit = 10;
    private static final String file = "test_data.csv";

    private static final Logger logger = LoggerFactory.getLogger(ForecasterTest.class);


    private void affichage() {
        System.out.println("\n---------------------------------------------------------------------------------------\n" +
                "      Forecasting Tests :    \n" +
                "---------------------------------------------------------------------------------------");
    }

    private static void printd(List<Double> inputs) {
        System.out.print("[");
        for (Double elmt : inputs) {
            System.out.printf("%.2f", elmt);
            System.out.print(", ");
        }
        System.out.println("]");
    }
    private static void printm(List<Measure> inputs) {
        System.out.print("[");
        for (Measure elmt : inputs) {
            System.out.printf("%.2f", elmt.getValue());
            System.out.print(", ");
        }
        System.out.println("]");
    }
    private static void printt(List<Measure> inputs) {
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
    private static List<Measure> getInputMeasureData(String file) throws IOException {
        return getDataFromFile("src/test/resources/data/"+ file);
    }

    /**
     * Collect data from resources/data directory
     *
     * @return list of values type=Double
     * @throws IOException
     */
    private static List<Double> getInputDoubleData(String file) throws IOException {
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
    private static List<Measure> getDataFromFile(String filePath) throws IOException {
        return MeasuresLoader.loadFromCSVFile(filePath);
    }

    /**
     * Compute the Root Mean Squared between 2 lists of double value
     *
     * @param measured data
     * @param forecasted data
     * @return RMS
     */
    private static double rootMeanSquaredErrorDouble (List<Double> measured, List<Double> forecasted) {
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
    private static double rootMeanSquaredErrorMeasure(List<Measure> measured, List<Measure> forecasted) {
        double mse = 0;
        for (int i=0;i<forecasted.size();i++) {
            mse += Math.pow(measured.get(i).getValue() - forecasted.get(i).getValue(), 2);
        }
        return Math.sqrt(mse / forecasted.size());
    }

    /**
     * create a csv file named 'algo'_java and write 'args' in the file
     *
     * @param algo name for the file
     * @param args data to write in the file
     * @throws IOException
     */
    private static void csvWriter(String algo, String args) throws IOException {
        FileWriter pw = null;
        File file = new File("src/test/resources/data/results_forecasting/" + algo + "_java.csv");
        try {
            pw = new FileWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String columnNamesList = "metric_id,mean_squared_error,training_time,inference_time";
        // No need give the headers Like: id, Name on builder.append
        String builder = columnNamesList + "\n" + args;
        assert pw != null;
        pw.write(builder);
        pw.close();
        System.out.println("done!");
    }

    /**
     * Iterate on all files in resources/data/split_data and forecasted the last 1/3 data of each file
     * then it save the result using csvWriter
     *
     * @param forecaster used to forecast
     * @param algo name of the forecaster
     * @throws IOException
     */
    private static void recorderNN(Forecaster forecaster, String algo) throws IOException {
        File filePath = new File("src/test/resources/data/split_data/");
        File[] files = filePath.listFiles();
        String result = "";
        for (File file : files) {
            if(file != null) {
                List<Measure> inputs = getInputMeasureData("split_data/" + file.getName());
                numPoints = (int) (inputs.size()*0.67);
                List<Measure> training = inputs.subList(0, inputs.size() - numPoints);
                List<Measure> validating = inputs.subList(inputs.size()-numPoints, inputs.size());

                double debut = System.currentTimeMillis();
                forecaster.fit(training, validating);
                double fin = System.currentTimeMillis();
                double fTime = (fin - debut)/1000;

                debut = System.currentTimeMillis();
                List<Measure> forecasted = forecaster.forecast(validating, numPoints);
                fin = System.currentTimeMillis();
                double iTime = (fin - debut)/1000;

                Double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
                if (!(mse.isNaN())) {
                    result = result + file.getName() +","+mse+","+fTime+","+iTime+"\n";
                }
            }
        }

        csvWriter(algo, result);
    }
    private static void recorderArima(Forecaster forecaster, String algo) throws IOException {
        File filePath = new File("src/test/resources/data/split_data/");
        File[] files = filePath.listFiles();
        String result = "";
        for (File file : files) {
            if(file != null) {
                List<Measure> inputs = getInputMeasureData("split_data/" + file.getName());
                numPoints = (int) (inputs.size()*0.67);
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

                Double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
                if (!(mse.isNaN())) {
                    result = result + file.getName() +","+mse+","+fTime+","+iTime+"\n";
                }
            }
        }

        csvWriter(algo, result);
    }

    @Test
    public void testArimaDoubleForecasting() throws IOException {
        System.out.println("\nARIMA_DOUBLE TEST :");
        Forecaster<Double> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.ARIMA_DOUBLE);

        /* TODO: boucler sur tous les fichiers dans le split_data
                 nom des fichiers : test_data i .csv
                 en iterrant sur i
         */

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

        Double mse = rootMeanSquaredErrorMeasure(validating, forecasted);
        System.out.println("Root Mean Squared value = " + mse);
        System.out.println("Fitting time : " + fTime + "s");
        System.out.println("Inference time : " + iTime + "s");

        Assertions.assertTrue(mse < limit, "Low precision MSE = " + mse + " > " + limit);
//        recorderArima(forecaster, "Arima");
    }

    @Test
    public void testLSTMForecasting() throws IOException {
        System.out.println("\nLSTM TEST :");

        Forecaster<Measure> forecaster = ForecasterFactory.getForecaster(ForecastingAlgorithm.LSTM);

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
//        recorderNN(forecaster, "Lstm");
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
//        recorderNN(forecaster, "Dnn");
    }
}
