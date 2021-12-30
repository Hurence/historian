package com.hurence.historian.greensights.util.solr;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.timeseries.model.Measure;

public class EnergyImpactMetricConverter {

    private static String METRIC_NAME = "energy_impact_kwh";

    public static Measure toMeasure(EnergyImpactMetric energyImpactMetric) {
        return Measure.builder()
                .timestamp(System.currentTimeMillis())
                .name(METRIC_NAME)
                .tags(energyImpactMetric.getLabels())
                .value(energyImpactMetric.getEnergyImpactInKwh())
                .build();
    }
}
