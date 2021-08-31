package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class DnnForecaster implements Forecaster<Measure>{

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
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(12345)
                .updater(new Nesterovs(0.005, 0.9))
                .l2(1e-4)
                .list()
                .layer(new DenseLayer.Builder() //create the first, input layer with xavier initialization
                        .nIn(1)
                        .nOut(16)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .layer(new DenseLayer.Builder()
                        .nIn(16)
                        .nOut(8)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .layer(new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .nIn(8)
                        .nOut(1)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .build();
        model = new MultiLayerNetwork(conf);
        model.getLayerWiseConfigurations().setValidateOutputLayerConfig(false);
        System.out.println("Initialize model....");
        model.init();
        System.out.println("Fit model....");
        model.fit(dsiTrain, 100);

        System.out.println("Evaluate model....");

          Evaluation eval = new Evaluation(3);
        while (dsiValid.hasNext()) {
            DataSet d = dsiValid.next();
            System.out.println(d);
            INDArray features = d.getFeatures();
            System.out.println(features);
            INDArray labels = d.getLabels();
            System.out.println(labels);
            INDArray predicted = model.output(features, false);
            System.out.println(predicted);
            eval.eval(labels, predicted);
        }
            System.out.println(eval.stats());
        /**/
    }

    public DataSetIterator toDataSetIterator(List<Measure> data, int numPoints, int batch) {
        INDArray input = Nd4j.create(numPoints, 1);
        INDArray output = Nd4j.create(numPoints, 1);
        int i = 0;
        for (Measure elm : data) {
            input.putScalar(i, elm.getTimestamp());
            output.putScalar(i, elm.getValue());
            i++;
        }
        DataSet dataSet = new DataSet( input, output );
        List<DataSet> listDataSet = dataSet.asList();
        return new ListDataSetIterator<>(listDataSet, batch);
    }


}
