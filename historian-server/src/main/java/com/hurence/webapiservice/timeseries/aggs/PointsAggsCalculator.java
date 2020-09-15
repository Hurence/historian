package com.hurence.webapiservice.timeseries.aggs;


import com.hurence.timeseries.modele.points.Point;
import com.hurence.webapiservice.modele.AGG;

import java.util.List;
import java.util.OptionalDouble;

public class PointsAggsCalculator extends AbstractAggsCalculator<Point> {

    public PointsAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    protected double calculSum(List<Point> points) {
        return points.stream().mapToDouble(Point::getValue).sum();
    }

    @Override
    protected OptionalDouble calculMin(List<Point> points) {
        return points.stream().mapToDouble(Point::getValue).min();
    }

    @Override
    protected OptionalDouble calculMax(List<Point> points) {
        return points.stream().mapToDouble(Point::getValue).max();
    }

    @Override
    protected long getCount(List<Point> points) {
        return points.size();
    }
}
