package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.hurence.webapiservice.modele.AGG.*;

public class PointsAggsCalculator implements AggsCalculator<Point> {

    private final List<AGG> askedAggList;
    private final List<AGG> neededAggList;
    private final Map<AGG, Number> aggValues = new HashMap<>();

    public PointsAggsCalculator(List<AGG> aggregList) {
        this.askedAggList = aggregList;
        this.neededAggList = calculNeededAggList(aggregList);
    }

    /**
     *
     * @param aggregList
     * @return list of aggs needed to be calculated. Indeed some aggs can be calculated using others.
     */
    private List<AGG> calculNeededAggList(List<AGG> aggregList) {
        List<AGG> neededAggList = new ArrayList<>(aggregList);
        if (aggregList.contains(AVG)){
            if(!aggregList.contains(SUM))
                neededAggList.add(SUM);
            if (!aggregList.contains(COUNT))
                neededAggList.add(COUNT);
            neededAggList.remove(AVG);
        }
        return neededAggList;
    }

    /**
     *
     * @return The aggs as a Json object if there is any requested otherwise return empty.
     */
    public Optional<JsonObject> getAggsAsJson() {
        calculAvgIfNeeded();
        JsonObject askedAggValuesAsJsonObject = new JsonObject();
        aggValues.forEach((key, value) -> {
            if (askedAggList.contains(key))
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
        if (askedAggList.contains(AVG))
            aggValues.put(AVG, BigDecimal.valueOf(aggValues.get(SUM).doubleValue())
                .divide(BigDecimal.valueOf(aggValues.get(COUNT).doubleValue()), RoundingMode.HALF_UP));
    }

    public void updateAggs(List<Point> points) {
        neededAggList.forEach(agg -> {
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
        double min;
        if (points.stream().mapToDouble(Point::getValue).min().isPresent()) {
            min = points.stream().mapToDouble(Point::getValue).min().getAsDouble();
            if (aggValues.containsKey(MIN)) {
                double currentMin = aggValues.get(MIN).doubleValue();
                min = Math.min(min, currentMin);
            }
            aggValues.put(MIN, min);
        }
    }
    private void calculateMax(List<Point> points) {
        double max;
        if (points.stream().mapToDouble(Point::getValue).max().isPresent()) {
            max = points.stream().mapToDouble(Point::getValue).max().getAsDouble();
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
