package com.hurence.historian.greensights.model;


import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Data
public class EnergyImpactReport {


    private final String rootUrl;
    private final String pagePath;
    private final String startDate;
    private final String endDate;

    private final Long transferredBytes;
    private final Integer pageViews;
    private final Long avgTimeOnPageInSec;
    private final Long avgPageSizeInBytes;
    private final Double energyImpactInKwh;
    private final Double energyImpactByPageInKwh;
    private final Double co2EqInKg;
    private final Double co2EqByPageInKg;


    public EnergyImpactReport(String rootUrl, String pagePath, String startDate, String endDate, List<WebPageEnergyImpactMetric> metrics, String... labels) {
        this.rootUrl = rootUrl;
        this.pagePath = pagePath;

        this.startDate = startDate;
        this.endDate = endDate;

        energyImpactInKwh = metrics.stream()
                .map(WebPageEnergyImpactMetric::getEnergyImpactInKwh)
                .reduce(0.0, Double::sum);

        pageViews = metrics.stream()
                .map(WebPageEnergyImpactMetric::getPageViews)
                .reduce(0, Integer::sum);

        avgTimeOnPageInSec = metrics.stream()
                .map(WebPageEnergyImpactMetric::getAvgTimeOnPageInSec)
                .reduce(0L, Long::sum) / metrics.size();

        transferredBytes = pageViews * metrics.stream()
                .map(WebPageEnergyImpactMetric::getPageSizeInBytes)
                .reduce(0L, Long::sum);

        co2EqInKg = metrics.stream()
                .map(WebPageEnergyImpactMetric::getCo2EqInKg)
                .reduce(0.0, Double::sum);

        avgPageSizeInBytes = (long) (transferredBytes / (double) pageViews);
        energyImpactByPageInKwh = energyImpactInKwh / (double) pageViews;
        co2EqByPageInKg = co2EqInKg / (double) pageViews;
    }


    public Map<String, String> getLabels() {

        Map<String, String> labels;
        labels = new HashMap<>();
        labels.put("root_url", rootUrl);
        if (pagePath == null) {
            labels.put("scope", "site");
        }else{
            labels.put("scope", "page");
            labels.put("page_path", pagePath);
        }

        return labels;
    }
}
