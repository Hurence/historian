package com.hurence.historian.greensights.scheduler;

import java.io.IOException;
import java.util.List;


import com.hurence.historian.greensights.model.request.ComputeRequest;
import com.hurence.historian.greensights.service.EnergyImpactComputationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GreensightsScheduler {

    private static final Logger log = LogManager.getLogger(GreensightsScheduler.class);

    @Autowired
    private EnergyImpactComputationService energyImpactComputationService;

    @Value("${greensights.analytics.dateRange.startDate:7daysAgo}")
    private String startDate;

    @Value("${greensights.analytics.dateRange.endDate:today}")
    private String endDate;

    @Value("#{'${greensights.analytics.rootUrlFilters:}'.split(',')}")
    private List<String> rootUrlFilters;


    @Value("#{'${greensights.analytics.accountFilters:}'.split(',')}")
    private List<String> accountFilters;


    @Scheduled(fixedRateString = "${greensights.scraper.scheduledDelayMs}")
    public void fetchMetrics() throws IOException {

        ComputeRequest computeRequest = new ComputeRequest(startDate, endDate, true, true, false, rootUrlFilters, accountFilters);
        energyImpactComputationService.compute(computeRequest);
    }


}