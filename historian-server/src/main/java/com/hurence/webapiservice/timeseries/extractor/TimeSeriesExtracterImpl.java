package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.model.Chunk;
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

    final Sampler<Measure> sampler;
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
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<Chunk> chunks) {
        List<Measure> measures = decompressPoints(from, to, chunks);
        List<Measure> sampledMeasures = sampler.sample(measures);
        this.sampledMeasures.addAll(sampledMeasures);
        aggsCalculator.updateAggs(measures);
    }

    @Override
    protected Optional<JsonObject> getAggsAsJson() {
        return aggsCalculator.getAggsAsJson();
    }

    private List<Measure> decompressPoints(long from, long to, List<Chunk> chunks) {
        Stream<Measure> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        Stream<Measure> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(Measure::getTimestamp));
        return sortedPoints.collect(Collectors.toList());
    }
}
