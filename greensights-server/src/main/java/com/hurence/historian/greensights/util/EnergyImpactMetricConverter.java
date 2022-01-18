package com.hurence.historian.greensights.util;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.timeseries.model.Measure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EnergyImpactMetricConverter {



    public static List<Measure> toMeasures(EnergyImpactReport energyImpactReport) {

        List<Measure> measures = new ArrayList<>();


        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("energy_impact_kwh_total")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getTotalEnergyImpactInKwh())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("co2_eq_kg_total")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getCo2EqInKg())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("page_size_bytes_total")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getTotalTransferredBytes())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("page_views_total")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getTotalPageViews())
                .build());

        measures.add(Measure.builder()
                .timestamp(DateUtils.fromDateRequest(energyImpactReport.getStartDate()).getTime())
                .name("avg_time_on_page_sec_total")
                .tags(energyImpactReport.getLabels())
                .value(energyImpactReport.getAvgTimeOnPageInSec())
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

}
