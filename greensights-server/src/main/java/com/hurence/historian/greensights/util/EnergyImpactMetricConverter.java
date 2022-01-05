package com.hurence.historian.greensights.util;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.timeseries.model.Measure;

public class EnergyImpactMetricConverter {

    private static String METRIC_NAME = "energy_impact_kwh";

    public static Measure toMeasure(EnergyImpactMetric energyImpactMetric) {

        return Measure.builder()
                .timestamp(energyImpactMetric.getMetricDate().getTime())
                .name(METRIC_NAME)
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getEnergyImpactInKwh())
                .build();
    }

}
