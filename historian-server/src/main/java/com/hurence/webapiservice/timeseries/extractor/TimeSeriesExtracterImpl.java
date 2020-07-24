package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.modele.PointImpl;
import com.hurence.timeseries.sampling.Sampler;
import com.hurence.timeseries.sampling.SamplerFactory;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.aggs.PointsAggsCalculator;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<PointImpl> sampler;
    final PointsAggsCalculator aggsCalculator;

    public TimeSeriesExtracterImpl(long from, long to,
                                   SamplingConf samplingConf,
                                   long totalNumberOfPoint,
                                   List<AGG> aggregList) {
        super(from, to, samplingConf, totalNumberOfPoint);
        sampler = SamplerFactory.getPointSampler(this.samplingConf.getAlgo(), this.samplingConf.getBucketSize());
        aggsCalculator = new PointsAggsCalculator(aggregList);
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks) {
        List<PointImpl> points = decompressPoints(from, to, chunks);
        List<PointImpl> sampledPoints = sampler.sample(points);
        this.sampledPoints.addAll(sampledPoints);
        aggsCalculator.updateAggs(points);
    }

    @Override
    protected Optional<JsonObject> getAggsAsJson() {
        return aggsCalculator.getAggsAsJson();
    }

    private List<PointImpl> decompressPoints(long from, long to, List<JsonObject> chunks) {
        Stream<PointImpl> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        Stream<PointImpl> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(PointImpl::getTimestamp));
        return sortedPoints.collect(Collectors.toList());
    }
}
