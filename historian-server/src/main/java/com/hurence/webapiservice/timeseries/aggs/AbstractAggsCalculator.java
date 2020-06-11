package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.DoubleStream;

import static com.hurence.historian.modele.HistorianFields.*;
import static com.hurence.webapiservice.modele.AGG.*;

public abstract class AbstractAggsCalculator<T> implements AggsCalculator<T> {

    private final Set<AGG> askedAggSet = new HashSet<>();
    private final Set<AGG> neededAggSet;
    final Map<AGG, Number> aggValues = new HashMap<>();

    public AbstractAggsCalculator(List<AGG> aggregList) {
        this.askedAggSet.addAll(aggregList);
        this.neededAggSet = calculNeededAggSet();
    }

    /**
     *
     * @return list of aggs needed to be calculated. Indeed some aggs can be calculated using others.
     */
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
    @Override
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

    /**
     * add Avg If needed
     */
    private void calculAvgIfNeeded() {
        if (askedAggSet.contains(AVG))
            aggValues.put(AVG, BigDecimal.valueOf(aggValues.get(SUM).doubleValue())
                    .divide(BigDecimal.valueOf(aggValues.get(COUNT).doubleValue()), RoundingMode.HALF_UP));
    }

    /**
     * update aggs values for whose that are cumulative (ie not avg)
     * @param elementsToAgg
     */
    @Override
    public void updateAggs(List<T> elementsToAgg) {
        if (!elementsToAgg.isEmpty())
            neededAggSet.forEach(agg -> {
                switch (agg) {
                    case SUM:
                        calculateSum(elementsToAgg);
                        break;
                    case MIN:
                        calculateMin(elementsToAgg);
                        break;
                    case MAX:
                        calculateMax(elementsToAgg);
                        break;
                    case COUNT:
                        calculateCount(elementsToAgg);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported aggregation: " + agg);
                }
            });
    }

    private void calculateSum(List<T> elementsToAgg) {
        double sum = getDoubleStreamFromElementsToAgg(elementsToAgg, RESPONSE_CHUNK_SUM_FIELD)
                .sum();
        if(aggValues.containsKey(SUM)) {
            double currentSum = aggValues.get(SUM).doubleValue();
            Number updatedSum =  BigDecimal.valueOf(currentSum+sum);
            aggValues.put(SUM, updatedSum);
        }else {
            aggValues.put(SUM, sum);
        }
    }
    private void calculateMin(List<T> elementsToAgg) {
        OptionalDouble minMap = getDoubleStreamFromElementsToAgg(elementsToAgg, RESPONSE_CHUNK_MIN_FIELD)
                .min();
        if (minMap.isPresent()) {
            double min = minMap.getAsDouble();
            if(aggValues.containsKey(MIN)) {
                double currentMin = aggValues.get(MIN).doubleValue();
                min = Math.min(min, currentMin);
            }
            aggValues.put(MIN, min);
        }
    }
    private void calculateMax(List<T> elementsToAgg) {
        OptionalDouble maxMap = getDoubleStreamFromElementsToAgg(elementsToAgg, RESPONSE_CHUNK_MAX_FIELD)
                .max();
        if (maxMap.isPresent()) {
            double max = maxMap.getAsDouble();
            if(aggValues.containsKey(MAX)) {
                double currentMax = aggValues.get(MAX).doubleValue();
                max = Math.max(max, currentMax);
            }
            aggValues.put(MAX, max);
        }
    }
    private void calculateCount(List<T> elementsToAgg) {
        double count = getDoubleCount(elementsToAgg);
        if(aggValues.containsKey(COUNT)) {
            double currentCount = aggValues.get(COUNT).doubleValue();
            Number newCount =  BigDecimal.valueOf(currentCount + count);
            aggValues.put(COUNT, newCount);
        }else {
            aggValues.put(COUNT, count);
        }
    }

    protected abstract DoubleStream getDoubleStreamFromElementsToAgg(List<T> elementsToAgg, String field);
    protected abstract double getDoubleCount(List<T> elementsToAgg);
}
