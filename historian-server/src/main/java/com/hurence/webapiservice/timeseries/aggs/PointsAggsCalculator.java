package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.timeseries.modele.points.PointImpl;
import com.hurence.webapiservice.modele.AGG;

import java.util.List;
import java.util.OptionalDouble;

public class PointsAggsCalculator extends AbstractAggsCalculator<PointImpl> {

    public PointsAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected double calculSum(List<PointImpl> points) {
        return points.stream().mapToDouble(PointImpl::getValue).sum();
    }

    @Override
    protected OptionalDouble calculMin(List<PointImpl> points) {
        return points.stream().mapToDouble(PointImpl::getValue).min();
    }

    @Override
    protected OptionalDouble calculMax(List<PointImpl> points) {
        return points.stream().mapToDouble(PointImpl::getValue).max();
    }

    @Override
    protected long getCount(List<PointImpl> points) {
        return points.size();
    }
}
