package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.evaluation.classification.Evaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.AdaGrad;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class DnnForecaster implements Forecaster<Measure>{

    @Override
    public List<Measure> forecast(List<Measure> inputs, int numPoints) {

        System.out.println("Split into training/validating....");
        List<Measure> training = inputs.subList(0, inputs.size() - numPoints);
        List<Measure> validating = inputs.subList(inputs.size()- numPoints, inputs.size());

        System.out.println("Create DataSetIterator....");
        DataSetIterator dsiTrain = getDataSetIterator(training, training.size(), 100);
        DataSetIterator dsiValid = getDataSetIterator(validating, validating.size(), 10);

        System.out.println("Create model....");
        MultiLayerConfiguration conf = createModel();
        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.getLayerWiseConfigurations().setValidateOutputLayerConfig(false);
        System.out.println("Initialize model....");
        model.init();
        System.out.println("Fit model....");
        model.fit(dsiTrain, 100);

        System.out.println("Evaluate model....");

        Evaluation eval = model.evaluate(dsiValid);
        System.out.println(eval.stats());

//        Evaluation eval = new Evaluation(numPoints);
//        while (dsiValid.hasNext()) {
//            DataSet t = dsiValid.next();
//            INDArray features = t.getFeatures();
//            INDArray labels = t.getLabels();
//            INDArray predicted = model.output(features, false);
//            eval.eval(labels, predicted);
//        }
//
//        System.out.println(eval.stats());
        return Collections.emptyList();
    }

    public DataSetIterator getDataSetIterator(List<Measure> data, int numPoints, int batch) {
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
     * Create a MutliLayerConfiguration that will be use to create the DNN model
     *
     * @return
     */
       public MultiLayerConfiguration createModel() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(12345) //include a random seed for reproducibility
                // use stochastic gradient descent as an optimization algorithm
                .updater(new Nesterovs(0.006, 0.9))
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
        return conf;
    }
}
