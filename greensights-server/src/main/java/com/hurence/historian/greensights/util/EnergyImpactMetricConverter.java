package com.hurence.historian.greensights.util;

import com.hurence.historian.greensights.model.WebPageEnergyImpactMetric;
import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.historian.greensights.model.UserWebBrowsingMetric;
import com.hurence.timeseries.model.Measure;

import java.util.ArrayList;
import java.util.List;

public class EnergyImpactMetricConverter {



    public static List<Measure> toMeasures(EnergyImpactReport energyImpactReport) {

        List<Measure> measures = new ArrayList<>();


        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("energy_impact_kwh")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getEnergyImpactInKwh())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("co2_eq_kg")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getCo2EqInKg())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("transferred_bytes")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getTransferredBytes())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("page_views")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getPageViews())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("avg_time_on_page_sec")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getAvgTimeOnPageInSec())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("energy_impact_by_page_kwh")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getEnergyImpactByPageInKwh())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("co2_eq_by_page_kg")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getCo2EqByPageInKg())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("avg_page_size_bytes")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getAvgPageSizeInBytes())
                .build());

        return measures;
    }

    public static List<Measure> toMeasures(WebPageEnergyImpactMetric webPageEnergyImpactMetric) {

        List<Measure> measures = new ArrayList<>();

        measures.add(Measure.builder()
                .timestamp(webPageEnergyImpactMetric.getMetricDate().getTime())
                .name("energy_impact_kwh")
                .tags(webPageEnergyImpactMetric.getLabels())
                .value(webPageEnergyImpactMetric.getEnergyImpactInKwh())
                .build());

        measures.add(Measure.builder()
                .timestamp(webPageEnergyImpactMetric.getMetricDate().getTime())
                .name("co2_eq_kg")
                .tags(webPageEnergyImpactMetric.getLabels())
                .value(webPageEnergyImpactMetric.getCo2EqInKg())
                .build());

        measures.add(Measure.builder()
                .timestamp(webPageEnergyImpactMetric.getMetricDate().getTime())
                .name("page_size_bytes")
                .tags(webPageEnergyImpactMetric.getLabels())
                .value(webPageEnergyImpactMetric.getPageSizeInBytes())
                .build());

        measures.add(Measure.builder()
                .timestamp(webPageEnergyImpactMetric.getMetricDate().getTime())
                .name("page_views")
                .tags(webPageEnergyImpactMetric.getLabels())
                .value(webPageEnergyImpactMetric.getPageViews())
                .build());

        measures.add(Measure.builder()
                .timestamp(webPageEnergyImpactMetric.getMetricDate().getTime())
                .name("avg_time_on_page_sec")
                .tags(webPageEnergyImpactMetric.getLabels())
                .value(webPageEnergyImpactMetric.getAvgTimeOnPageInSec())
                .build());

        return measures;
    }


    public static List<Measure> toMeasures(UserWebBrowsingMetric metric) {

        List<Measure> measures = new ArrayList<>();

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("transferred_data_bytes")
                .tags(metric.getLabels())
                .value(metric.getTransferredDataInBytes())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("time_spent_browsing_sec")
                .tags(metric.getLabels())
                .value(metric.getTimeSpentBrowsingInSec())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("energy_impact_kwh")
                .tags(metric.getLabels())
                .value(metric.getEnergyImpactInKwh())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("co2_eq_kg")
                .tags(metric.getLabels())
                .value(metric.getCo2EqInKg())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("domain_views")
                .tags(metric.getLabels())
                .value(metric.getDomainViews())
                .build());



        return measures;
    }

}
