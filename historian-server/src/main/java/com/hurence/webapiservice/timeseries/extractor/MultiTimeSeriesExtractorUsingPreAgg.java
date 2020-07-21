package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.webapiservice.modele.SamplingConf;

import java.util.List;

public class MultiTimeSeriesExtractorUsingPreAgg extends MultiTimeSeriesExtracterImpl {

    public MultiTimeSeriesExtractorUsingPreAgg(long from, long to, SamplingConf samplingConf,
                                               List<MetricRequest> metricRequests, boolean returnQuality) {
        super(from, to, samplingConf, metricRequests, returnQuality);
    }

    @Override
    protected TimeSeriesExtracter createTimeSeriesExtractor(MetricRequest metricRequest) {
        return new TimeSeriesExtracterUsingPreAgg(from, to, samplingConf, totalNumberOfPointByMetrics.get(metricRequest), aggregList, returnQuality);
    }
}