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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<Point> sampler;
    List<AGG> aggListForAvg = new ArrayList<>();
    Map<AGG, Double> aggregationsMap = new HashMap<>();

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
        List<Point> points = sortedPoints.collect(Collectors.toList());
        List<Point> sampledPoints = sampler.sample(points);
        this.sampledPoints.addAll(sampledPoints);
        aggListForAvg.addAll(aggregList);
        if(aggregList.contains(AVG)){
            aggListForAvg.remove(AVG);
            if (!aggListForAvg.contains(SUM))
                aggListForAvg.add(SUM);
            if (!aggListForAvg.contains(COUNT))
                aggListForAvg.add(COUNT);
            calculateAggreg(points);
            calculateAvg();
        }else {
            calculateAggreg(points);
        }
        getAggregationsMap();
    }

    private void getAggregationsMap() {
        aggregList.forEach(agg -> aggregValuesMap.put(agg, aggregationsMap.get(agg)));
        aggListForAvg = new ArrayList<>();
    }

    private void calculateAvg() {
        double avg = BigDecimal.valueOf(aggregationsMap.get(SUM))
                .divide(BigDecimal.valueOf(aggregationsMap.get(COUNT)), 3, RoundingMode.HALF_UP)
                .doubleValue();
        aggregationsMap.put(AVG, avg);
    }

    protected void calculateAggreg(List<Point> points) {
        if (!points.isEmpty())
            aggListForAvg.forEach(agg -> {
                switch (agg) {
                    case SUM:
                        double sum = 0;
                        for (Point point : points) {
                            sum += point.getValue();
                        }
                        double aggValue = sum;
                        if(aggregationsMap.containsKey(SUM)) {
                            double currentSum = aggregationsMap.get(SUM);
                            double newSum =  BigDecimal.valueOf(currentSum+aggValue)
                                    .doubleValue();
                            aggregationsMap.put(SUM, newSum);
                        }else {
                            aggregationsMap.put(SUM, aggValue);
                        }
                        break;
                    case MIN:
                        double min = points.get(0).getValue();
                        for (Point point : points) {
                            double next = point.getValue();
                            min = Math.min(min, next);
                        }
                        aggValue = min;
                        if(aggregationsMap.containsKey(MIN)) {
                            double currentMin = aggregationsMap.get(MIN);
                            if (aggValue < currentMin) {
                                aggregationsMap.put(MIN, aggValue);
                            }
                        }else {
                            aggregationsMap.put(MIN, aggValue);
                        }
                        break;
                    case MAX:
                        double max = points.get(0).getValue();
                        for (Point point : points) {
                            double next = point.getValue();
                            max = Math.max(max, next);
                        }
                        aggValue = max;
                        if(aggregationsMap.containsKey(MAX)) {
                            double currentMax = aggregationsMap.get(MAX);
                            if (aggValue > currentMax) {
                                aggregationsMap.put(MAX, aggValue);
                            }

                        }else {
                            aggregationsMap.put(MAX, aggValue);
                        }
                        break;
                    case COUNT:
                        aggValue = points.size();
                        if(aggregationsMap.containsKey(COUNT)) {
                            double currentCount = aggregationsMap.get(COUNT);
                            double newCount =  BigDecimal.valueOf(currentCount + aggValue)
                                    .doubleValue();
                            aggregationsMap.put(COUNT, newCount);
                        }else {
                            aggregationsMap.put(COUNT, aggValue);
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unsupported aggregation: " + agg);
                }
            });
    }
}
