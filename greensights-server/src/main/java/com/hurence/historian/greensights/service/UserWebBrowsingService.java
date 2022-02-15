package com.hurence.historian.greensights.service;

import com.hurence.historian.greensights.model.UserWebBrowsingMetric;
import com.hurence.historian.greensights.util.EnergyImpactMetricConverter;
import com.hurence.timeseries.model.Measure;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentLinkedQueue;

@Service
@RequiredArgsConstructor
public class UserWebBrowsingService {

    private final ConcurrentLinkedQueue<Measure> updateQueue;

    private static final Logger log = LogManager.getLogger(UserWebBrowsingService.class);

    /**
     * Convert that user web browsing stats to an historian measure and add it to the queue
     * @param metric
     * @return
     */
    public UserWebBrowsingMetric save(UserWebBrowsingMetric metric){

        log.info("putting user browsing metrics to update queue");
        updateQueue.addAll(EnergyImpactMetricConverter.toMeasures(metric) );
        return metric;
    }

}