package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.hurence.webapiservice.modele.AGG.*;

public class PointsAggsCalculator implements AggsCalculator<Point> {

    private final Set<AGG> askedAggSet = new HashSet<>();
    private final Set<AGG> neededAggSet;
    private final Map<AGG, Number> aggValues = new HashMap<>();

    public PointsAggsCalculator(List<AGG> aggregList) {
        this.askedAggSet.addAll(aggregList);
        this.neededAggSet = calculNeededAggList();
    }

    /**
     *
     * @return list of aggs needed to be calculated. Indeed some aggs can be calculated using others.
     */
    private Set<AGG> calculNeededAggList() {
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

    /**
     * add Avg If needed
     */
    private void calculAvgIfNeeded() {
        if (askedAggSet.contains(AVG))
            aggValues.put(AVG, BigDecimal.valueOf(aggValues.get(SUM).doubleValue())
                .divide(BigDecimal.valueOf(aggValues.get(COUNT).doubleValue()), RoundingMode.HALF_UP));
    }

    public void updateAggs(List<Point> points) {
        if (!points.isEmpty())
            neededAggSet.forEach(agg -> {
                switch (agg) {
                    case SUM:
                        calculateSum(points);
                        break;
                    case MIN:
                        calculateMin(points);
                        break;
                    case MAX:
                        calculateMax(points);
                        break;
                    case COUNT:
                        calculateCount(points);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported aggregation: " + agg);
                }
            });
    }

    /**
     * update sum value to aggValues
     */
    private void calculateSum(List<Point> points) {
        double sum = points.stream().mapToDouble(Point::getValue).sum();
        if (aggValues.containsKey(SUM)) {
            double currentSum = aggValues.get(SUM).doubleValue();
            Number updatedSum =  BigDecimal.valueOf(currentSum+sum);
            aggValues.put(SUM, updatedSum);
        } else {
            aggValues.put(SUM, sum);
        }
    }

    /**
     * update min value to aggValues
     */
    private void calculateMin(List<Point> points) {
        OptionalDouble minMap = points.stream().mapToDouble(Point::getValue).min();
        if (minMap.isPresent()) {
            double min = minMap.getAsDouble();
            if (aggValues.containsKey(MIN)) {
                double currentMin = aggValues.get(MIN).doubleValue();
                min = Math.min(min, currentMin);
            }
            aggValues.put(MIN, min);
        }
    }
    private void calculateMax(List<Point> points) {
        OptionalDouble maxMap = points.stream().mapToDouble(Point::getValue).max();
        if (maxMap.isPresent()) {
            double max = maxMap.getAsDouble();
            if (aggValues.containsKey(MAX)) {
                double currentMax = aggValues.get(MAX).doubleValue();
                max = Math.max(max, currentMax);
            }
            aggValues.put(MAX, max);
        }
    }
    private void calculateCount(List<Point> points) {
        double count = points.size();
        if(aggValues.containsKey(COUNT)) {
            double currentCount = aggValues.get(COUNT).doubleValue();
            double newCount =  BigDecimal.valueOf(currentCount + count)
                    .doubleValue();
            aggValues.put(COUNT, newCount);
        }else {
            aggValues.put(COUNT, count);
        }
    }
}
