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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class EnergyImpactComputationService {
    private static final Logger log = LogManager.getLogger(EnergyImpactComputationService.class);

    private final ConcurrentLinkedQueue<Measure> updateQueue;
    private final GoogleAnalyticsService googleAnalyticsService;
    private final WebPageActivityAnalysisRepository webPageActivityAnalysisRepository;

    /**
     * will consolidate a report given some metrics
     *
     * @param computeRequest
     * @return
     */
    public List<EnergyImpactReport> compute(ComputeRequest computeRequest) {

        log.debug("fetching metrics");

        // get metrics from google analytics
        List<EnergyImpactMetric> energyImpactMetrics = googleAnalyticsService.retrieveMetrics(computeRequest);

        // compute report by sites
        Map<String, List<EnergyImpactMetric>> metricsBySite = energyImpactMetrics.stream()
                .collect(Collectors.groupingBy(EnergyImpactMetric::getRootUrl));


        return metricsBySite.keySet().stream().map(rootUrl -> {

            EnergyImpactReport siteReport = new EnergyImpactReport(
                    rootUrl,
                    null,
                    computeRequest.getStartDate(),
                    computeRequest.getEndDate(),
                    metricsBySite.get(rootUrl));

            log.info("-----------------------------------------");
            log.info("Report for site : " + siteReport.getRootUrl() +
                    " between " + computeRequest.getStartDate() + " and " + computeRequest.getEndDate());
            log.info("page views       :              " + siteReport.getPageViews());
            log.info("transferred data in Mb :        " + siteReport.getTransferredBytes() / 1024.0 / 1024.0);
            log.info("energy impact in Kwh :          " + siteReport.getEnergyImpactInKwh());
            log.info("co2 equivalence in Kg :         " + siteReport.getCo2EqInKg());
            log.info("---");
            log.info("average page size in Mb :       " + siteReport.getAvgPageSizeInBytes() / 1024.0 / 1024.0);
            log.info("energy impact / page in Kwh :   " + siteReport.getEnergyImpactByPageInKwh());
            log.info("co2 equivalence / page in Kg :  " + siteReport.getCo2EqByPageInKg());
            log.info("average time on page in sec :   " + siteReport.getAvgTimeOnPageInSec());
            log.info("-----------------------------------------");

            // save all these metrics
            if (computeRequest.getDoSaveMetrics()) {
                log.info("saving metrics");
                webPageActivityAnalysisRepository.saveAll(
                        metricsBySite.get(rootUrl)
                                .stream()
                                .map(WebPageActivityAnalysis::fromEnergyImpactMetric)
                                .collect(Collectors.toList())
                );
            }

            // save all historian measures
            if (computeRequest.getDoSaveMeasures()) {
                // convert them to measures
                List<Measure> measures = metricsBySite.get(rootUrl)
                        .stream()
                        .flatMap(metric -> EnergyImpactMetricConverter.toMeasures(metric).stream())
                        .collect(Collectors.toList());

                // compute report by site and by Page
                Map<String, List<EnergyImpactMetric>> metricBySiteAndByPage = metricsBySite.get(rootUrl)
                        .stream()
                        .collect(Collectors.groupingBy(EnergyImpactMetric::getPagePath));

                List<Measure> pageReportMeasures = metricBySiteAndByPage.keySet()
                        .stream()
                        .flatMap(pagePath -> EnergyImpactMetricConverter.toMeasures(
                                new EnergyImpactReport(
                                        rootUrl,
                                        pagePath,
                                        computeRequest.getStartDate(),
                                        computeRequest.getEndDate(),
                                        metricBySiteAndByPage.get(pagePath))
                        ).stream())
                        .collect(Collectors.toList());

                log.info("measures are being sent to historian");
                updateQueue.addAll(EnergyImpactMetricConverter.toMeasures(siteReport));
                updateQueue.addAll(pageReportMeasures);
                updateQueue.addAll(measures);
            }
            return siteReport;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }
}
