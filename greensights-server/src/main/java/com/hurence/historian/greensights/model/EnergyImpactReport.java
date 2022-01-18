package com.hurence.historian.greensights.model;


import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Data
public class EnergyImpactReport {


    private final String rootUrl;
    private final String startDate;
    private final String endDate;
    private final double energyImpactByPage;
    private final Long totalTransferredBytes;
    private final Double totalEnergyImpactInKwh;
    private final Integer totalPageViews;
    private final Double co2EqInKg;
    private final Long avgTimeOnPageInSec;

    public EnergyImpactReport(String rootUrl, String startDate, String endDate, List<EnergyImpactMetric> metrics) {
        this.rootUrl = rootUrl;

        this.startDate = startDate;
        this.endDate = endDate;

        totalEnergyImpactInKwh = metrics.stream()
                .map(EnergyImpactMetric::getEnergyImpactInKwh)
                .reduce(0.0, Double::sum);

        totalPageViews = metrics.stream()
                .map(EnergyImpactMetric::getPageViews)
                .reduce(0, Integer::sum);

        avgTimeOnPageInSec = metrics.stream()
                .map(EnergyImpactMetric::getAvgTimeOnPageInSec)
                .reduce(0L, Long::sum) / metrics.size();


        totalTransferredBytes = totalPageViews * metrics.stream()
                .map(EnergyImpactMetric::getPageSizeInBytes)
                .reduce(0L, Long::sum) ;

        co2EqInKg = metrics.stream()
                .map(EnergyImpactMetric::getCo2EqInKg)
                .reduce(0.0, Double::sum);

        energyImpactByPage = totalEnergyImpactInKwh / (double) totalPageViews;

    }


    public Map<String, String> getLabels() {
        HashMap<String, String> labels = new HashMap<>();
        labels.put("root_url", rootUrl);
        return labels;
    }
}
