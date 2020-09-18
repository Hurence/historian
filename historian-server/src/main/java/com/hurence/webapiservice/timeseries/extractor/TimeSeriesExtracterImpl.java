package com.hurence.webapiservice.timeseries.extractor;



import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import com.hurence.timeseries.sampling.Sampler;
import com.hurence.timeseries.sampling.SamplerFactory;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import com.hurence.webapiservice.timeseries.aggs.PointsAggsCalculator;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<Measure> sampler;
    final PointsAggsCalculator aggsCalculator;
    final Float qualityLimit;

    public TimeSeriesExtracterImpl(long from, long to,
                                   SamplingConf samplingConf,
                                   long totalNumberOfPoint,
                                   List<AGG> aggregList,
                                   boolean returnQuality,
                                   Float qualityLimit) {
        super(from, to, samplingConf, totalNumberOfPoint, returnQuality);
        sampler = SamplerFactory.getPointSamplerWithQuality(this.samplingConf.getAlgo(), this.samplingConf.getBucketSize());
        aggsCalculator = new PointsAggsCalculator(aggregList);
        if(!qualityLimit.isNaN())
            this.qualityLimit = qualityLimit;
        else
            this.qualityLimit = 0f;
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<Chunk> chunks) {
        List<Measure> points = decompressPoints(from, to, chunks);
        List<Measure> sampledPoints = sampler.sample(points);
        List<Measure> filteredPoints = filterPointsByQuality(sampledPoints);
        this.sampledMeasures.addAll(filteredPoints);
        aggsCalculator.updateAggs(points);
    }

    private List<Measure> filterPointsByQuality(List<Measure> sampledPoints) {
        List<Measure> pointsToReturn = new ArrayList<>();
        sampledPoints.forEach(point -> {
            if ((point.hasQuality() && point.getQuality() >= qualityLimit)
                    || (!point.hasQuality()))
                pointsToReturn.add(point);
        });
        return pointsToReturn;
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