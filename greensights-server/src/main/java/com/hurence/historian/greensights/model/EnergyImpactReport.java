package com.hurence.historian.greensights.model;

import com.hurence.historian.greensights.util.solr.EnergyImpactMetricConverter;
import com.hurence.timeseries.model.Measure;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class EnergyImpactReport {

    private List<EnergyImpactMetric> metrics;


    public double getTotalTransferredMegaBytes(){

        return getPageViewsGlobal() * metrics.stream()
                .map(EnergyImpactMetric::getPageSizeInBytes)
                .reduce(0L, Long::sum) /1024.0 /1024.0;
    }

    public double getEnergyImpactByPage(){

        return getEnergyImpactInKwhGlobal() / (double)getPageViewsGlobal();
    }

    public double getEnergyImpactInKwhGlobal(){

        return metrics.stream()
                .map(EnergyImpactMetric::getEnergyImpactInKwh)
                .reduce(0.0, Double::sum);
    }

    public long getPageViewsGlobal(){

        return metrics.stream()
                .map(EnergyImpactMetric::getPageViews)
                .reduce(0L, Long::sum);
    }

    public double getCo2EqInKg(){

        return metrics.stream()
                .map(EnergyImpactMetric::getCo2EqInKg)
                .reduce(0.0, Double::sum);
    }

    public List<Measure> getMeasures(){
        return metrics.stream()
                .map(EnergyImpactMetricConverter::toMeasure)
                .collect(Collectors.toList());
    }
}
