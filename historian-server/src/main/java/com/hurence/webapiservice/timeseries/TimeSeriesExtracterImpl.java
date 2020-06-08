package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<Point> sampler;

    public TimeSeriesExtracterImpl(String metricName, long from, long to,
                                   SamplingConf samplingConf,
                                   long totalNumberOfPoint,
                                   List<AGG> aggregList) {
        super(metricName, from, to, samplingConf, totalNumberOfPoint, aggregList);
        sampler = SamplerFactory.getPointSampler(this.samplingConf.getAlgo(), this.samplingConf.getBucketSize());
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks) {
        Stream<Point> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        Stream<Point> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(Point::getTimestamp));
        this.points = sortedPoints.collect(Collectors.toList());
        List<Point> sampledPoints = sampler.sample(points);
        this.sampledPoints.addAll(sampledPoints);
        calculateAggreg();
    }

    @Override
    protected void calculateAggreg() {
        if (!points.isEmpty())
            aggregList.forEach(agg -> {
                double aggValue;
                switch (agg) {
                    case AVG:
                        long numberOfPoint = points.size();
                        double current = 0;
                        for (Point sampledPoint : points) {
                            current += sampledPoint.getValue();
                        }
                        aggValue = BigDecimal.valueOf(current)
                                .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                                .doubleValue();
                        if(aggregValuesMap.containsKey(AVG)) {
                            double currentAvg = aggregValuesMap.get(AVG);
                            double newAvg =  BigDecimal.valueOf(currentAvg+aggValue)
                                    .divide(BigDecimal.valueOf(2), 3, RoundingMode.HALF_UP)
                                    .doubleValue();
                            aggregValuesMap.put(AVG, newAvg);
                        }else {
                            aggregValuesMap.put(AVG, aggValue);
                        }
                        break;
                    case SUM:
                        double sum = 0;
                        for (Point sampledPoint : points) {
                            sum += sampledPoint.getValue();
                        }
                        aggValue = sum;
                        if(aggregValuesMap.containsKey(SUM)) {
                            double currentSum = aggregValuesMap.get(SUM);
                            double newSum =  BigDecimal.valueOf(currentSum+aggValue)
                                    .doubleValue();
                            aggregValuesMap.put(SUM, newSum);
                        }else {
                            aggregValuesMap.put(SUM, aggValue);
                        }
                        break;
                    case MIN:
                        double min = points.get(0).getValue();
                        for (Point sampledPoint : points) {
                            double next = sampledPoint.getValue();
                            if (next < min) {
                                min = next;
                            }
                        }
                        aggValue = min;
                        if(aggregValuesMap.containsKey(MIN)) {
                            double currentMin = aggregValuesMap.get(MIN);
                            double newMin = 0;
                            if (aggValue < currentMin) {
                                newMin = aggValue;
                            }
                            aggregValuesMap.put(MIN, newMin);
                        }else {
                            aggregValuesMap.put(MIN, aggValue);
                        }
                        break;
                    case MAX:
                        double max = points.get(0).getValue();
                        for (Point sampledPoint : points) {
                            double next = sampledPoint.getValue();
                            if (next > max) {
                                max = next;
                            }
                        }
                        aggValue = max;
                        if(aggregValuesMap.containsKey(MAX)) {
                            double currentMax = aggregValuesMap.get(MAX);
                            double newMax = 0;
                            if (aggValue < currentMax) {
                                newMax = aggValue;
                            }
                            aggregValuesMap.put(MAX, newMax);
                        }else {
                            aggregValuesMap.put(MAX, aggValue);
                        }
                        break;
                    case COUNT:
                        aggValue = points.size();
                        if(aggregValuesMap.containsKey(COUNT)) {
                            double currentCount = aggregValuesMap.get(COUNT);
                            double newCount =  BigDecimal.valueOf(currentCount + aggValue)
                                    .doubleValue();
                            aggregValuesMap.put(COUNT, newCount);
                        }else {
                            aggregValuesMap.put(COUNT, aggValue);
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unsupported aggregation: " + agg);
                }
            });
    }
}
