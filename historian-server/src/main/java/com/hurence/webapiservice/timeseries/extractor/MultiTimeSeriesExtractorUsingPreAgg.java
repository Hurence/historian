package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.webapiservice.modele.SamplingConf;

import java.util.List;

public class MultiTimeSeriesExtractorUsingPreAgg extends MultiTimeSeriesExtracterImpl {

    public MultiTimeSeriesExtractorUsingPreAgg(long from, long to, SamplingConf samplingConf,
                                               List<MetricRequest> metricRequests) {
        super(from, to, samplingConf, metricRequests);
    }

    @Override
    protected TimeSeriesExtracter createTimeSeriesExtractor(MetricRequest metricRequest) {
        return new TimeSeriesExtracterUsingPreAgg(from, to, samplingConf, totalNumberOfPointByMetrics.get(metricRequest), aggregList);
    }
}