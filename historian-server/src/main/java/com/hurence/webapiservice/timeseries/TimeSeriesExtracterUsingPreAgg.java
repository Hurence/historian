package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class TimeSeriesExtracterUsingPreAgg extends AbstractTimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUsingPreAgg.class);

    List<AGG> aggregationListWithoutAVG = new ArrayList<>();
    Map<AGG, Double> preliminaryAggregationsMap = new HashMap<>();

    public TimeSeriesExtracterUsingPreAgg(String metricName, long from, long to, SamplingConf samplingConf, long totalNumberOfPoint, List<AGG> aggregList) {
        super(metricName, from, to, samplingConf, totalNumberOfPoint, aggregList);
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks) {
        List<Point> sampledPoint = extractPoints(chunks);
        this.sampledPoints.addAll(sampledPoint);
        calculateAggregations();
        fillingAggregValuesMapAndResetAggListWithAvg();
    }

    /**
     * if we have AVG in the aggregList, we will calculate the aggregations without the AVG and with SUM and COUNT added if not exist,then calculate the AVG
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

    /**
     * filling finalAggregationsMap which has the final aggregations response to return and reset the aggListWithAvg which used to calculate the average
     */
    private void fillingAggregValuesMapAndResetAggListWithAvg() {

    }

    /**
     * calculate the AVG from the SUM and the COUNT.
     */
    private void calculateAvg() {

    }

    protected void calculateAggregWithoutAVG() {
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
        double aggValue = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                .sum();
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
        double aggValue = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD))
                .min()
                .getAsDouble();
        if(preliminaryAggregationsMap.containsKey(MIN)) {
            double currentMin = preliminaryAggregationsMap.get(MIN);
            double newMin = 0;
            if (aggValue < currentMin) {
                newMin = aggValue;
            }
            preliminaryAggregationsMap.put(MIN, newMin);
        }else {
            preliminaryAggregationsMap.put(MIN, aggValue);
        }
    }
    private void calculateMax() {
        double aggValue = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MAX_FIELD))
                .max()
                .getAsDouble();
        if(preliminaryAggregationsMap.containsKey(MAX)) {
            double currentMax = preliminaryAggregationsMap.get(MAX);
            double newMax = 0;
            if (aggValue < currentMax) {
                newMax = aggValue;
            }
            preliminaryAggregationsMap.put(MAX, newMax);
        }else {
            preliminaryAggregationsMap.put(MAX, aggValue);
        }
    }
    private void calculateCount() {
        double aggValue = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_COUNT_FIELD))
                .sum();
        if(preliminaryAggregationsMap.containsKey(COUNT)) {
            double currentCount = preliminaryAggregationsMap.get(COUNT);
            double newCount =  BigDecimal.valueOf(currentCount + aggValue)
                    .doubleValue();
            preliminaryAggregationsMap.put(COUNT, newCount);
        }else {
            preliminaryAggregationsMap.put(COUNT, aggValue);
        }
    }
    private List<Point> extractPoints(List<JsonObject> chunks) {
        List<List<JsonObject>> groupedChunks = groupChunks(chunks, this.samplingConf.getBucketSize());
        return groupedChunks.stream()
                .map(this::sampleChunksIntoOneAggPoint)
                .sorted(Comparator.comparing(Point::getTimestamp))
                .collect(Collectors.toList());
    }

    private List<List<JsonObject>> groupChunks(List<JsonObject> chunks, int bucketSize) {
        List<List<JsonObject>> groupedChunks = new ArrayList<>();
        int currentPointNumber = 0;
        List<JsonObject> bucketOfChunks = new ArrayList<>();
        for (JsonObject chunk: chunks) {
            bucketOfChunks.add(chunk);
            currentPointNumber += chunk.getInteger(RESPONSE_CHUNK_COUNT_FIELD);
            if (currentPointNumber >= bucketSize) {
                groupedChunks.add(bucketOfChunks);
                bucketOfChunks = new ArrayList<>();
                currentPointNumber = 0;
            }
        }
        if (!bucketOfChunks.isEmpty())
            groupedChunks.add(bucketOfChunks);
        return groupedChunks;
    }

    private Point sampleChunksIntoOneAggPoint(List<JsonObject> chunks) {
        if (chunks.isEmpty())
            throw new IllegalArgumentException("chunks can not be empty !");
        LOGGER.trace("sampling chunks (showing first one) : {}", chunks.get(0).encodePrettily());
        long timestamp = chunks.stream()
                .mapToLong(chunk -> chunk.getLong(RESPONSE_CHUNK_START_FIELD))
                .findFirst()
                .getAsLong();
        double aggValue;
        switch (samplingConf.getAlgo()) {
            case AVERAGE:
                double sum = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                        .sum();
                long numberOfPoint = chunks.stream()
                        .mapToLong(chunk -> chunk.getLong(RESPONSE_CHUNK_COUNT_FIELD))
                        .sum();
                aggValue = BigDecimal.valueOf(sum)
                        .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                        .doubleValue();
                break;
            case FIRST:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_FIRST_VALUE_FIELD))
                        .findFirst()
                        .getAsDouble();
                break;
            case MIN:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD))
                        .min()
                        .getAsDouble();
                break;
            case MAX:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MAX_FIELD))
                        .max()
                        .getAsDouble();
                break;
            case MODE_MEDIAN:
            case LTTB:
            case MIN_MAX:
            case NONE:
            default:
                throw new IllegalStateException("Unsupported algo: " + samplingConf.getAlgo());
        }
        return new Point(0, timestamp, aggValue);
    }
}
