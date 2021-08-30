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

//        while (dsiValid.hasNext()) {
//            System.out.println(dsiValid.next());
//        }
//        Evaluation eval = model.evaluate(dsiValid);
//        System.out.println(eval.stats());


        Evaluation eval = new Evaluation(numPoints);
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
        return Collections.emptyList();
    }

    public DataSetIterator getDataSetIterator(List<Measure> data, int numPoints, int batch) {
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

    /**
     * Create a MutliLayerConfiguration that will be use to create the DNN model
     *
     * @return MultiLayerConfiguration
     */
       public MultiLayerConfiguration createModel() {
           return new NeuralNetConfiguration.Builder()
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
    }
}
