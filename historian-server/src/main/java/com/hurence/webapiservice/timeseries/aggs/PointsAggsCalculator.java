package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.hurence.webapiservice.modele.AGG.*;

public class PointsAggsCalculator extends AbstractAggsCalculator<Point> {

    public PointsAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    /**
     * update sum value to aggValues
     */
    protected void calculateSum(List<Point> points) {
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
    protected void calculateMin(List<Point> points) {
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
    protected void calculateMax(List<Point> points) {
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
    protected void calculateCount(List<Point> points) {
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
