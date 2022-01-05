package com.hurence.historian.greensights.util;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.timeseries.model.Measure;

import java.util.ArrayList;
import java.util.List;

public class EnergyImpactMetricConverter {


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
