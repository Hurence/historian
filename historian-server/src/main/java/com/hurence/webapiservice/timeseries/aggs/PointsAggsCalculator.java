package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;


import java.util.*;
import java.util.stream.DoubleStream;

public class PointsAggsCalculator extends AbstractAggsCalculator<Point> {

    public PointsAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected DoubleStream getDoubleStreamFromElementsToAgg(List<Point> points, String field) {
        return points.stream().mapToDouble(Point::getValue);
    }

    @Override
    protected double getDoubleCount(List<Point> points) {
        return points.size();
    }
}
