package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class TimeSeriesExtracterUsingPreAgg extends AbstractTimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterUsingPreAgg.class);

    public TimeSeriesExtracterUsingPreAgg(String metricName, long from, long to, SamplingConf samplingConf, long totalNumberOfPoint, List<AGG> aggregList) {
        super(metricName, from, to, samplingConf, totalNumberOfPoint, aggregList);
    }

    @Override
    protected void samplePointsFromChunksAndCalculAggreg(long from, long to, List<JsonObject> chunks) {
        List<Point> sampledPoint = extractPoints(chunks);
        this.sampledPoints.addAll(sampledPoint);
        calculateAggreg();
    }

    protected void calculateAggreg() {
        aggregList.forEach(agg -> {
            double aggValue;
            switch (agg) {
                case AVG:
                    double sum = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                            .sum();
                    long numberOfPoint = chunks.stream()
                            .mapToLong(chunk -> chunk.getLong(RESPONSE_CHUNK_COUNT_FIELD))
                            .sum();
                    aggValue = BigDecimal.valueOf(sum)
                            .divide(BigDecimal.valueOf(numberOfPoint), 3, RoundingMode.HALF_UP)
                            .doubleValue();
                    long newNumberOfPoint = this.currentNumberOfPoints + numberOfPoint;
                    if(aggregValuesMap.containsKey(AVG)) {
                        double currentAvg = aggregValuesMap.get(AVG);
                        double oldTotalValue = BigDecimal.valueOf(currentAvg)
                                .multiply(BigDecimal.valueOf(currentNumberOfPoints))
                                .doubleValue();
                        double newAvg =  BigDecimal.valueOf(sum+oldTotalValue)
                                .divide(BigDecimal.valueOf(newNumberOfPoint), 3, RoundingMode.HALF_UP)
                                .doubleValue();
                        aggregValuesMap.put(AVG, newAvg);
                    }else {
                        aggregValuesMap.put(AVG, aggValue);
                    }
                    this.currentNumberOfPoints = newNumberOfPoint;
                    break;
                case SUM:
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                            .sum();
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
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD))
                            .min()
                            .getAsDouble();
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
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MAX_FIELD))
                            .max()
                            .getAsDouble();
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
                    aggValue = chunks.stream()
                            .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_COUNT_FIELD))
                            .sum();
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
