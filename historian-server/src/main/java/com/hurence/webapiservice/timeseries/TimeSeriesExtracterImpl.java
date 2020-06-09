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

import static com.hurence.webapiservice.modele.AGG.*;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<Point> sampler;
    List<AGG> aggregationListWithoutAVG = new ArrayList<>();
    Map<AGG, Double> preliminaryAggregationsMap = new HashMap<>();
    List<Point> points = new ArrayList<>();

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
        points = sortedPoints.collect(Collectors.toList());
        List<Point> sampledPoints = sampler.sample(points);
        this.sampledPoints.addAll(sampledPoints);
        calculateAggregations();
        fillingAggregValuesMapAndResetAggListWithAvg();
    }

    /**
     * filling finalAggregationsMap which has the final aggregations response to return and reset the aggregationListWithoutAVG which used to calculate the average
     */
    private void fillingAggregValuesMapAndResetAggListWithAvg() {
        aggregList.forEach(agg -> finalAggregationsMap.put(agg, preliminaryAggregationsMap.get(agg)));
        aggregationListWithoutAVG = new ArrayList<>();
    }

    /**
     * calculate the AVG from the SUM and the COUNT.
     */
    private void calculateAvg() {
        double avg = BigDecimal.valueOf(preliminaryAggregationsMap.get(SUM))
                .divide(BigDecimal.valueOf(preliminaryAggregationsMap.get(COUNT)), 3, RoundingMode.HALF_UP)
                .doubleValue();
        preliminaryAggregationsMap.put(AVG, avg);
    }

    /**
     * if we have AVG in the aggregList, we will calculate the aggregations without the AVG ,and with SUM and COUNT added if not exist,then calculate the AVG
     * else, we just calculate the aggregations.
     * either way we store the aggregations values in preliminaryAggregationsMap which is not always what we want to return.
     */
    private void calculateAggregations() {
        aggregationListWithoutAVG.addAll(aggregList);
        if(aggregList.contains(AVG)){
            aggregationListWithoutAVG.remove(AVG);
            if (!aggregationListWithoutAVG.contains(SUM))
                aggregationListWithoutAVG.add(SUM);
            if (!aggregationListWithoutAVG.contains(COUNT))
                aggregationListWithoutAVG.add(COUNT);
            calculateAggregWithoutAVG();
            calculateAvg();
        }else {
            calculateAggregWithoutAVG();
        }
    }

    protected void calculateAggregWithoutAVG() {
        if (!points.isEmpty())
            aggregationListWithoutAVG.forEach(agg -> {
                switch (agg) {
                    case SUM:
                        calculateSum();
                        break;
                    case MIN:
                        calculateMin();
                        break;
                    case MAX:
                        calculateMax();
                        break;
                    case COUNT:
                        calculateCount();
                        break;
                    default:
                        throw new IllegalStateException("Unsupported aggregation: " + agg);
                }
            });
    }

    private void calculateSum() {
        double sum = 0;
        for (Point point : points) {
            sum += point.getValue();
        }
        double aggValue = sum;
        if(preliminaryAggregationsMap.containsKey(SUM)) {
            double currentSum = preliminaryAggregationsMap.get(SUM);
            double newSum =  BigDecimal.valueOf(currentSum+aggValue)
                    .doubleValue();
            preliminaryAggregationsMap.put(SUM, newSum);
        }else {
            preliminaryAggregationsMap.put(SUM, aggValue);
        }
    }
    private void calculateMin() {
        double min = points.get(0).getValue();
        for (Point point : points) {
            double next = point.getValue();
            min = Math.min(min, next);
        }
        double aggValue = min;
        if(preliminaryAggregationsMap.containsKey(MIN)) {
            double currentMin = preliminaryAggregationsMap.get(MIN);
            if (aggValue < currentMin) {
                preliminaryAggregationsMap.put(MIN, aggValue);
            }
        }else {
            preliminaryAggregationsMap.put(MIN, aggValue);
        }
    }
    private void calculateMax() {
        double max = points.get(0).getValue();

        for (Point point : points) {
            double next = point.getValue();
            max = Math.max(max, next);
        }
        double aggValue = max;
        if(preliminaryAggregationsMap.containsKey(MAX)) {
            double currentMax = preliminaryAggregationsMap.get(MAX);
            if (aggValue > currentMax) {
                preliminaryAggregationsMap.put(MAX, aggValue);
            }

        }else {
            preliminaryAggregationsMap.put(MAX, aggValue);
        }
    }
    private void calculateCount() {
        double aggValue = points.size();
        if(preliminaryAggregationsMap.containsKey(COUNT)) {
            double currentCount = preliminaryAggregationsMap.get(COUNT);
            double newCount =  BigDecimal.valueOf(currentCount + aggValue)
                    .doubleValue();
            preliminaryAggregationsMap.put(COUNT, newCount);
        }else {
            preliminaryAggregationsMap.put(COUNT, aggValue);
        }
    }
}
