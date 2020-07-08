package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.timeseries.modele.PointImpl;
import com.hurence.webapiservice.modele.AGG;

import java.util.List;
import java.util.stream.DoubleStream;

public class PointsAggsCalculator extends AbstractAggsCalculator<PointImpl> {

    public PointsAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected DoubleStream getDoubleStreamFromElementsToAgg(List<PointImpl> points, String field) {
        return points.stream().mapToDouble(PointImpl::getValue);
    }

    @Override
    protected double getDoubleCount(List<PointImpl> points) {
        return points.size();
    }
}
