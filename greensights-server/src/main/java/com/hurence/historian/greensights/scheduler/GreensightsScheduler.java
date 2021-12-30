package com.hurence.historian.greensights.scheduler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;


import com.hurence.historian.greensights.model.EnergyImpactMetric;
import com.hurence.historian.greensights.model.EnergyImpactReport;
import com.hurence.historian.greensights.service.GoogleAnalyticsService;
import com.hurence.historian.greensights.service.PageSizeService;
import com.hurence.timeseries.model.Measure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GreensightsScheduler {

    private static final Logger log = LogManager.getLogger(GreensightsScheduler.class);

    @Autowired
    private BlockingQueue<Measure> updateQueue;

    @Autowired
    private GoogleAnalyticsService googleAnalyticsService;

    @Autowired
    private PageSizeService pageSizeService;

    @Scheduled(fixedRateString ="${scraper.scheduledDelayMs}")
    public void fetchMetrics() throws IOException {
        log.debug("fetching metrics");

        // get metrics from google analytics
        List<EnergyImpactMetric> energyImpactMetrics = googleAnalyticsService.retrieveMetrics();



        EnergyImpactReport energyImpactReport = new EnergyImpactReport();
        energyImpactReport.setMetrics(energyImpactMetrics);

        log.info("energy impact in Kwh : " + energyImpactReport.getEnergyImpactInKwhGlobal());
        log.info("kg co2 : " + energyImpactReport.getCo2EqInKg());
        log.info("total page views : " + energyImpactReport.getPageViewsGlobal());
        log.info("energy impact in Kwh / page: " + energyImpactReport.getEnergyImpactByPage());
        log.info("total transferred MB " + energyImpactReport.getTotalTransferredMegaBytes());



        // convert them to measures
        List<Measure> measures = energyImpactReport.getMeasures();

        // add measures to queue for being indexed to historian
        log.info("measures are not sent to historian yet");
       // updateQueue.addAll(measures);
    }



}