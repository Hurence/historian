package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class ChunkAggsCalculator extends AbstractAggsCalculator<JsonObject> {

    public ChunkAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    /**
     * update sum value to aggValues
     */
    @Override
    protected void calculateSum(List<JsonObject> chunks) {
        double sum = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                .sum();
        if(aggValues.containsKey(SUM)) {
            double currentSum = aggValues.get(SUM).doubleValue();
            Number updatedSum =  BigDecimal.valueOf(currentSum+sum);
            aggValues.put(SUM, updatedSum);
        }else {
            aggValues.put(SUM, sum);
        }
    }

    /**
     * update min value to aggValues
     */
    @Override
    protected void calculateMin(List<JsonObject> chunks) {
        OptionalDouble minMap = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD)).min();
        if (minMap.isPresent()) {
            double min = minMap.getAsDouble();
            if(aggValues.containsKey(MIN)) {
                double currentMin = aggValues.get(MIN).doubleValue();
                min = Math.min(min, currentMin);
            }
            aggValues.put(MIN, min);
        }
    }
    @Override
    protected void calculateMax(List<JsonObject> chunks) {
        OptionalDouble maxMap = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MAX_FIELD)).max();
        if (maxMap.isPresent()) {
            double max = maxMap.getAsDouble();
            if(aggValues.containsKey(MAX)) {
                double currentMax = aggValues.get(MAX).doubleValue();
                max = Math.max(max, currentMax);
            }
            aggValues.put(MAX, max);
        }
    }
    @Override
    protected void calculateCount(List<JsonObject> chunks) {
        double count = chunks.stream()
                .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_COUNT_FIELD))
                .sum();
        if(aggValues.containsKey(COUNT)) {
            double currentCount = aggValues.get(COUNT).doubleValue();
            Number newCount =  BigDecimal.valueOf(currentCount + count);
            aggValues.put(COUNT, newCount);
        }else {
            aggValues.put(COUNT, count);
        }
    }

}
