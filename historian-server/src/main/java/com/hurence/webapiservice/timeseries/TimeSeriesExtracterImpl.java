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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.webapiservice.modele.AGG.*;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<Point> sampler;
    List<AGG> aggListForAvg = new ArrayList<>();
    Map<AGG, Double> aggregationsMap = new HashMap<>();
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
     * filling aggregValuesMap which has the final aggregations response to return and reset the aggListWithAvg which used to calculate the average
     */
    private void fillingAggregValuesMapAndResetAggListWithAvg() {

    }

    /**
     * calculate the AVG from the SUM and the COUNT.
     */
    private void calculateAvg() {

    }

    /**
     * if we have AVG in the aggregList, we will calculate the aggregations withou the AVG and with SUM and COUNT added if not exist,then calculate the AVG
     * else, we just calculate the aggregations.
     * either way we store the aggregations values in aggListForAvg which is not always what we want to return.
     */
    private void calculateAggregations() {
        aggListForAvg.addAll(aggregList);
        if(aggregList.contains(AVG)){
            aggListForAvg.remove(AVG);
            if (!aggListForAvg.contains(SUM))
                aggListForAvg.add(SUM);
            if (!aggListForAvg.contains(COUNT))
                aggListForAvg.add(COUNT);
            calculateAggregWithoutAVG();
            calculateAvg();
        }else {
            calculateAggregWithoutAVG();
        }
    }

    protected void calculateAggregWithoutAVG() {
        if (!points.isEmpty())
            aggListForAvg.forEach(agg -> {
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
        if(aggregationsMap.containsKey(SUM)) {
            double currentSum = aggregationsMap.get(SUM);
            double newSum =  BigDecimal.valueOf(currentSum+aggValue)
                    .doubleValue();
            aggregationsMap.put(SUM, newSum);
        }else {
            aggregationsMap.put(SUM, aggValue);
        }
    }
    private void calculateMin() {
        double min = points.get(0).getValue();
        for (Point point : points) {
            double next = point.getValue();
            min = Math.min(min, next);
        }
        double aggValue = min;
        if(aggregationsMap.containsKey(MIN)) {
            double currentMin = aggregationsMap.get(MIN);
            if (aggValue < currentMin) {
                aggregationsMap.put(MIN, aggValue);
            }
        }else {
            aggregationsMap.put(MIN, aggValue);
        }
    }
    private void calculateMax() {
        double max = points.get(0).getValue();

        for (Point point : points) {
            double next = point.getValue();
            max = Math.max(max, next);
        }
        double aggValue = max;
        if(aggregationsMap.containsKey(MAX)) {
            double currentMax = aggregationsMap.get(MAX);
            if (aggValue > currentMax) {
                aggregationsMap.put(MAX, aggValue);
            }

        }else {
            aggregationsMap.put(MAX, aggValue);
        }
    }
    private void calculateCount() {
        double aggValue = points.size();
        if(aggregationsMap.containsKey(COUNT)) {
            double currentCount = aggregationsMap.get(COUNT);
            double newCount =  BigDecimal.valueOf(currentCount + aggValue)
                    .doubleValue();
            aggregationsMap.put(COUNT, newCount);
        }else {
            aggregationsMap.put(COUNT, aggValue);
        }
    }
}
