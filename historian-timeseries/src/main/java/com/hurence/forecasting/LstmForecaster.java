package com.hurence.forecasting;

import com.hurence.timeseries.model.Measure;


import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.ObjectUtils;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.*;
import org.deeplearning4j.nn.conf.layers.*;
import org.deeplearning4j.nn.conf.layers.recurrent.LastTimeStep;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.evaluation.classification.Evaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.LabelLastTimeStepPreProcessor;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;


public class LstmForecaster implements Forecaster<Measure>{

    private MultiLayerNetwork model;
    private int lookback;

    @Override
    public List<Measure> forecast(List<Measure> inputs, int numPoints) throws IOException {

        if(model == null)
            throw new IOException("model must be initialized first, did you really call fit method before forecasting ?");

        DataSetIterator inputDataIterator = toDataSetIterator(inputs.subList(2, inputs.size()), 10);
        DataSetIterator inputDataIterator2 = toDataSetIterator(inputs.subList(2, inputs.size()), 10);
        INDArray output = model.output(inputDataIterator);

//        while (inputDataIterator2.hasNext()) {
//            System.out.println(inputDataIterator2.next());
//        }

        // TODO compute time step to fill timestamp part of the measure
        List<Measure> forecasted = new ArrayList<>();
        double[] doubleVector = output.toDoubleVector();
        int i = 0;
        for (double value : doubleVector ) {
            forecasted.add(Measure.fromValue(inputs.get(inputs.size() - numPoints + i).getTimestamp(), value));
            i++;
        }

        // TODO loop over numPoints to return more than 1 forecasted point
        return forecasted;
    }

    @Override
    public void fit(List<Measure> trainingData, List<Measure> validatingData) throws IOException {
        if (lookback == 0) {
            if (validatingData.size() > 60) {
                lookback = 30;
            } else if ( validatingData.size() > 3) {
                lookback = validatingData.size() / 2 - 1;
            } else {
                lookback = 1;
            }
        }
        lookback=1; // ca marche mais on a que des 0

        DataSetIterator dsiTrain = toDataSetIterator(trainingData, 100);
        dsiTrain.setPreProcessor(new LabelLastTimeStepPreProcessor());

        MultiLayerConfiguration conf = createLSTMModel();
        model = new MultiLayerNetwork(conf);
        model.getLayerWiseConfigurations().setValidateOutputLayerConfig(false);

        model.init();
        model.fit(dsiTrain, 100);
    }

    /**
     * Create a 3D rank DataSetIterator from a list of measures
     *
     * @param inputData is the list of measures with at least a value and a timestamp
     * @param batch size
     * @return the DataSetIterator
     */
    public DataSetIterator toDataSetIterator(List<Measure> inputData, int batch) {
        List<Double> values = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        for (Measure inputDatum : inputData) {
            values.add(inputDatum.getValue());
            timestamps.add(inputDatum.getTimestamp());
        }
        INDArray input = Nd4j.create(values.size() - lookback +1, 1, lookback);
        INDArray output = Nd4j.create(values.size() - lookback +1, 1, 1);
        for (int i = 0; i < values.size() - lookback +1; i++) {
            for (int j = 0; j < lookback; j++) {
                input.putScalar(new int[]{i, 0, j}, values.get(i+j));
            }
            output.putScalar(new int[]{i, 0, 0}, timestamps.get(i+lookback-1));
        }
        DataSet dataSet = new DataSet( input, output );
        List<DataSet> listDataSet = dataSet.asList();
        return new ListDataSetIterator<DataSet>( listDataSet, batch );
    }


    /**
     * Create a MutliLayerConfiguration that will be use to create the LSTM model
     *
     * @return a MultiLayerConfiguration
     */
    public MultiLayerConfiguration createLSTMModel() {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(12345)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .updater(new Nesterovs(0.005, 0.9))
                .l2(1e-4)
                .list()
                .layer(0, new LastTimeStep(new LSTM.Builder()
                        .nIn(1)
                        .nOut(8)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .build()))
                .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .nIn(8)
                        .nOut(1)
                        .activation(Activation.RELU)
                        .weightInit(WeightInit.XAVIER)
                        .build())
                .backpropType(BackpropType.Standard)
                .build();
        return conf;
    }
}
