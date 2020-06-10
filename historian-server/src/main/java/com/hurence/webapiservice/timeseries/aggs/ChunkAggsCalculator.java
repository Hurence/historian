package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public class ChunkAggsCalculator implements AggsCalculator<JsonObject> {

    private final Set<AGG> askedAggSet = new HashSet<>();
    private final Set<AGG> neededAggSet;
    private final Map<AGG, Number> aggValues = new HashMap<>();


    public ChunkAggsCalculator(List<AGG> aggregList) {
        this.askedAggSet.addAll(aggregList);
        this.neededAggSet = calculNeededAggSet();
    }

    private Set<AGG> calculNeededAggSet() {
        Set<AGG> neededAggSet = new HashSet<>(askedAggSet);
        if (neededAggSet.remove(AVG)){
            neededAggSet.add(SUM);
            neededAggSet.add(COUNT);
        }
        return neededAggSet;
    }

    /**
     *
     * @return The aggs as a Json object if there is any requested otherwise return empty.
     */
    public Optional<JsonObject> getAggsAsJson() {
        calculAvgIfNeeded();
        JsonObject askedAggValuesAsJsonObject = new JsonObject();
        aggValues.forEach((key, value) -> {
            if (askedAggSet.contains(key))
                askedAggValuesAsJsonObject.put(key.toString(), value.doubleValue());
        });
        if (askedAggValuesAsJsonObject.isEmpty())
            return Optional.empty();
        else
            return Optional.of(askedAggValuesAsJsonObject);
    }

    private void calculAvgIfNeeded() {
        if (askedAggSet.contains(AVG))
            aggValues.put(AVG, BigDecimal.valueOf(aggValues.get(SUM).doubleValue())
                .divide(BigDecimal.valueOf(aggValues.get(COUNT).doubleValue()), RoundingMode.HALF_UP));
    }

    /**
     * update aggs values for whose that are cumulative (ie not avg)
     * @param chunks
     */
    public void updateAggs(List<JsonObject> chunks) {
        if (!chunks.isEmpty())
            neededAggSet.forEach(agg -> {
                switch (agg) {
                    case SUM:
                        calculateSum(chunks);
                        break;
                    case MIN:
                        calculateMin(chunks);
                        break;
                    case MAX:
                        calculateMax(chunks);
                        break;
                    case COUNT:
                        calculateCount(chunks);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported aggregation: " + agg);
                }
            });
    }

    /**
     * update sum value to aggValues
     */
    private void calculateSum(List<JsonObject> chunks) {
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
    private void calculateMin(List<JsonObject> chunks) {
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
    private void calculateMax(List<JsonObject> chunks) {
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
    private void calculateCount(List<JsonObject> chunks) {
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
