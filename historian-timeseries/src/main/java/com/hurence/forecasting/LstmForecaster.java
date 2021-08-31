package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;


import java.io.IOException;
import java.util.*;

import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.*;
import org.deeplearning4j.nn.conf.layers.*;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.evaluation.classification.Evaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;


public class LstmForecaster implements Forecaster<Measure>{

    private MultiLayerNetwork model;

    @Override
    public List<Measure> forecast(List<Measure> inputs, int numPoints) throws IOException {

        if(model == null)
            throw new IOException("model must be initialized first, did you really call fit method before forecasting ?");

        DataSetIterator inputDataIterator = toDataSetIterator(inputs, inputs.size(), 10);
        INDArray output = model.output(inputDataIterator);

        // TODO compute time step to fill timestamp part of the measure
        List<Measure> forecasted = new ArrayList<>();
        double[] doubleVector = output.toDoubleVector();
        for (double value : doubleVector ){
            forecasted.add(Measure.fromValue(0, value));
        }

        // TODO loop over numPoints to return more than 1 forecasted point
        return forecasted;
    }

    @Override
    public void fit(List<Measure> trainingData) {
        System.out.println("Split into training/validating....");
        List<Measure> training = trainingData.subList(0, trainingData.size() - 10);
        List<Measure> validating = trainingData.subList(trainingData.size()- 10, trainingData.size());

        System.out.println("Create DataSetIterator....");
        DataSetIterator dsiTrain = toDataSetIterator(training, training.size(), 100);
        DataSetIterator dsiValid = toDataSetIterator(validating, validating.size(), 10);

        System.out.println("Create model....");
        MultiLayerConfiguration conf = createLSTMModel();
        model = new MultiLayerNetwork(conf);
        model.getLayerWiseConfigurations().setValidateOutputLayerConfig(false);
        System.out.println("Initialize model....");
        model.init();
        System.out.println("Fit model....");
        model.fit(dsiTrain, 100);

        System.out.println("Evaluate model....");

        Evaluation eval = model.evaluate(dsiValid);
        System.out.println(eval.stats());
    }

    public DataSetIterator toDataSetIterator(List<Measure> data, int numPoints, int batch) {
        INDArray input = Nd4j.create(numPoints, 1);
        INDArray output = Nd4j.create(numPoints, 1);
        int i = 0;
        for (Measure elmt : data) {
            input.putScalar(i, elmt.getTimestamp());
            output.putScalar(i, elmt.getValue());
            i++;
        }
        DataSet dataSet = new DataSet( input, output );
        List<DataSet> listDataSet = dataSet.asList();
        return new ListDataSetIterator<DataSet>( listDataSet, batch );
    }

    /**
     * Transform a List<Double> into double[]
     *
     * @param input
     * @return
     */
    public double[] listToDouble(List<Double> input) {
        double[] dataArray = new double[input.size()];
        for (int i = 0; i < dataArray.length; i++) {
            dataArray[i] = input.get(i);
        }
        return dataArray;
    }


    /**
     * Transform a List<Measure> into a List<Double>
     *
     * @param input
     * @return
     */
    public List<Double> measureToDouble(List<Measure> input) {
        ArrayList<Double> result = new ArrayList<Double>();
        for (int i=0; i < input.size(); i++) {
            result.add(input.get(i).getValue());
        }
        return result;
    }

    /**
     * Create a MutliLayerConfiguration that will be use to create the LSTM model
     *
     * @return
     */
    public MultiLayerConfiguration createLSTMModel() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .seed(12345)
                .updater(new Nesterovs(0.005, 0.9))
                .l2(1e-4)
                .list()
                .layer(0, new DenseLayer.Builder()
                        .nIn(1)
                        .nOut(8)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .layer(1, new LSTM.Builder()
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .nIn(8)
                        .nOut(16)
                        .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)
                        .gradientNormalizationThreshold(10)
                        .build())
                .layer(2, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .nIn(16)
                        .nOut(1)
                        .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)
                        .gradientNormalizationThreshold(10)
                        .build())
                .build();
        return conf;
    }
}
