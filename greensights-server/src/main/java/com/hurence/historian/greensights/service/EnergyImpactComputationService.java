package com.hurence.historian.greensights.service;

import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.historian.greensights.model.request.ComputeRequest;
import com.hurence.historian.greensights.model.solr.WebPageActivityAnalysis;
import com.hurence.historian.greensights.repository.WebPageActivityAnalysisRepository;
import com.hurence.historian.greensights.util.EnergyImpactMetricConverter;
import com.hurence.timeseries.model.Measure;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class EnergyImpactComputationService {
    private static final Logger log = LogManager.getLogger(EnergyImpactComputationService.class);

    private final BlockingQueue<Measure> updateQueue;
    private final GoogleAnalyticsService googleAnalyticsService;
    private final WebPageActivityAnalysisRepository webPageActivityAnalysisRepository;

    /**
     * will consolidate a report given some metrics
     *
     * @param computeRequest
     * @return
     */
    public EnergyImpactReport compute(ComputeRequest computeRequest){

        log.debug("fetching metrics");

        // get metrics from google analytics
        List<EnergyImpactMetric> energyImpactMetrics = googleAnalyticsService.retrieveMetrics(computeRequest);
        EnergyImpactReport energyImpactReport = new EnergyImpactReport(
                computeRequest.getStartDate(),
                computeRequest.getEndDate(),
                energyImpactMetrics);

        log.info("energy impact in Kwh : " + energyImpactReport.getEnergyImpactInKwhGlobal());
        log.info("kg co2 : " + energyImpactReport.getCo2EqInKg());
        log.info("total page views : " + energyImpactReport.getPageViewsGlobal());
        log.info("energy impact in Kwh / page: " + energyImpactReport.getEnergyImpactByPage());
        log.info("total transferred MB " + energyImpactReport.getTotalTransferredMegaBytes());

        // save all these metrics
        if(computeRequest.getDoSaveMetrics()) {
            log.info("saving metrics");
            webPageActivityAnalysisRepository.saveAll(
                    energyImpactMetrics.stream()
                            .map(WebPageActivityAnalysis::fromEnergyImpactMetric)
                            .collect(Collectors.toList())
            );
        }

        // save all historian measures
        if(computeRequest.getDoSaveMeasures()){
            // convert them to measures
            List<Measure> measures = energyImpactMetrics.stream()
                    .flatMap(metric -> EnergyImpactMetricConverter.toMeasures(metric).stream())
                    .collect(Collectors.toList());

            // add measures to queue for being indexed to historian
            log.info("measures are being sent to historian");
            updateQueue.addAll(measures);
        }



        return energyImpactReport;
    }
}
