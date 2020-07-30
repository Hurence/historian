package com.hurence.webapiservice.timeseries.extractor;

import com.hurence.timeseries.modele.Point;
import com.hurence.timeseries.sampling.Sampler;
import com.hurence.timeseries.sampling.SamplerFactory;
import com.hurence.timeseries.modele.PointImpl;
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

    final Sampler<Point> sampler;
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
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks) {
        List<Point> points = decompressPoints(from, to, chunks);
        List<Point> sampledPoints = sampler.sample(points);
        List<Point> filteredPoints = filterPointsByQuality(sampledPoints);
        this.sampledPoints.addAll(filteredPoints);
        aggsCalculator.updateAggs(points);
    }

    private List<Point> filterPointsByQuality(List<Point> sampledPoints) {
        List<Point> pointsToReturn = new ArrayList<>();
        sampledPoints.forEach(point -> {
            if ((point.hasQuality() && point.getQuality() >= qualityLimit)
                        || (!point.hasQuality())) // TODO configure this to get default quality and compare with qualitylimit
                pointsToReturn.add(point);
        });
        return pointsToReturn;
    }

    @Override
    protected Optional<JsonObject> getAggsAsJson() {
        return aggsCalculator.getAggsAsJson();
    }

    private List<Point> decompressPoints(long from, long to, List<JsonObject> chunks) {
        Stream<Point> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        Stream<Point> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(Point::getTimestamp));
        return sortedPoints.collect(Collectors.toList());
    }
}
