package com.hurence.webapiservice.timeseries.aggs;

import com.hurence.timeseries.model.Measure;
import com.hurence.webapiservice.modele.AGG;

import java.util.List;
import java.util.OptionalDouble;

public class PointsAggsCalculator extends AbstractAggsCalculator<Measure> {

    public PointsAggsCalculator(List<AGG> aggregList) {
        super(aggregList);
    }

    @Override
    protected double calculSum(List<Measure> measures) {
        return measures.stream().mapToDouble(Measure::getValue).sum();
    }

    @Override
    protected OptionalDouble calculMin(List<Measure> measures) {
        return measures.stream().mapToDouble(Measure::getValue).min();
    }

    @Override
    protected OptionalDouble calculMax(List<Measure> measures) {
        return measures.stream().mapToDouble(Measure::getValue).max();
    }

    @Override
    protected long getCount(List<Measure> measures) {
        return measures.size();
    }
}
