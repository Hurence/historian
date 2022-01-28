package com.hurence.historian.greensights.util;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.historian.greensights.model.UserLastHourWebBrowsingMetric;
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

    public static List<Measure> toMeasures(EnergyImpactMetric energyImpactMetric) {

        List<Measure> measures = new ArrayList<>();

        measures.add(Measure.builder()
                .timestamp(energyImpactMetric.getMetricDate().getTime())
                .name("energy_impact_kwh")
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getEnergyImpactInKwh())
                .build());

        measures.add(Measure.builder()
                .timestamp(energyImpactMetric.getMetricDate().getTime())
                .name("co2_eq_kg")
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getCo2EqInKg())
                .build());

        measures.add(Measure.builder()
                .timestamp(energyImpactMetric.getMetricDate().getTime())
                .name("page_size_bytes")
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getPageSizeInBytes())
                .build());

        measures.add(Measure.builder()
                .timestamp(energyImpactMetric.getMetricDate().getTime())
                .name("page_views")
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getPageViews())
                .build());

        measures.add(Measure.builder()
                .timestamp(energyImpactMetric.getMetricDate().getTime())
                .name("avg_time_on_page_sec")
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getAvgTimeOnPageInSec())
                .build());

        return measures;
    }


    public static List<Measure> toMeasures(UserLastHourWebBrowsingMetric metric) {

        List<Measure> measures = new ArrayList<>();

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
                .name("needed_trees_eq")
                .tags(metric.getLabels())
                .value(metric.getNeededTreesEq())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("charged_smartphones_eq")
                .tags(metric.getLabels())
                .value(metric.getNumberOfChargedSmartphonesEq())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("kms_by_car_eq")
                .tags(metric.getLabels())
                .value(metric.getKmsByCarEq())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("avg_page_size_bytes")
                .tags(metric.getLabels())
                .value(metric.getAvgPageSizeInBytes())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("page_views")
                .tags(metric.getLabels())
                .value(metric.getPageViews())
                .build());

        measures.add(Measure.builder()
                .timestamp(metric.getMetricDate().getTime())
                .name("avg_time_on_page_sec")
                .tags(metric.getLabels())
                .value(metric.getAvgTimeOnPageInSec())
                .build());

        return measures;
    }

}
