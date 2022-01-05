package com.hurence.historian.greensights.model;


import lombok.Data;

import java.util.List;


@Data
public class EnergyImpactReport {


    private final String startDate;
    private final String endDate;
    private final double energyImpactByPage;
    private final double totalTransferredMegaBytes;
    private final Double energyImpactInKwhGlobal;
    private final Integer pageViewsGlobal;
    private final Double co2EqInKg;

    public EnergyImpactReport(String startDate, String endDate,List<EnergyImpactMetric> metrics) {

        this.startDate = startDate;
        this.endDate = endDate;

        energyImpactInKwhGlobal = metrics.stream()
                .map(EnergyImpactMetric::getEnergyImpactInKwh)
                .reduce(0.0, Double::sum);

        pageViewsGlobal = metrics.stream()
                .map(EnergyImpactMetric::getPageViews)
                .reduce(0, Integer::sum);

        totalTransferredMegaBytes = pageViewsGlobal * metrics.stream()
                .map(EnergyImpactMetric::getPageSizeInBytes)
                .reduce(0L, Long::sum) / 1024.0 / 1024.0;

        co2EqInKg = metrics.stream()
                .map(EnergyImpactMetric::getCo2EqInKg)
                .reduce(0.0, Double::sum);

        energyImpactByPage = energyImpactInKwhGlobal / (double) pageViewsGlobal;

    }


}
