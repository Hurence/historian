package com.hurence.logisland.timeseries.sampling;

import com.hurence.logisland.timeseries.sampling.record.*;

public class SamplerFactory {
    /**
     * Instanciates a sampler.
     *
     * @param algorithm the sampling algorithm
     * @param valueFieldName the name of the field containing the point value (Y)
     * @param timeFieldName the name of the field containing the point time (X)
     * @param parameter an int parameter
     * @return the sampler
     */
    public static RecordSampler getRecordSampler(SamplingAlgorithm algorithm,
                                                 String valueFieldName,
                                                 String timeFieldName,
                                                 int parameter) {

        switch (algorithm) {
            case LTTB:
                return new LTTBRecordSampler(valueFieldName, timeFieldName, parameter);
            case FIRST:
                return new FirstItemRecordSampler(valueFieldName, timeFieldName, parameter);
            case AVERAGE:
                return new AverageRecordSampler(valueFieldName, timeFieldName, parameter);
            case MIN_MAX:
                return new MinMaxRecordSampler(valueFieldName, timeFieldName, parameter);
            case MODE_MEDIAN:
                return new ModeMedianRecordSampler(valueFieldName, timeFieldName, parameter);
            case NONE:
                return new IsoRecordSampler(valueFieldName, timeFieldName);
            default:
                throw new UnsupportedOperationException("algorithm " + algorithm.name() + " is not yet supported !");
        }
    }
}
