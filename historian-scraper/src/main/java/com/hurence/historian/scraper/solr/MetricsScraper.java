package com.hurence.historian.scraper.solr;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.concurrent.BlockingQueue;

import com.hurence.historian.scraper.PrometheusScraper;
import com.hurence.historian.scraper.walkers.HistorianPrometheusMetricsWalker;
import com.hurence.historian.scraper.walkers.PrometheusMetricsWalker;
import com.hurence.timeseries.model.Measure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MetricsScraper {

    private static final Logger log = LogManager.getLogger(MetricsScraper.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    @Value("${scraper.url}"  )
    private String scraperUrl;

    @Autowired
    private BlockingQueue<Measure> updateQueue;

    @Scheduled(fixedRateString ="${scraper.scheduledDelayMs}")
    public void fetchMetrics() throws IOException {
        URL url = new URL(scraperUrl);
        PrometheusMetricsWalker walker  = new HistorianPrometheusMetricsWalker(updateQueue);
        log.info("What's up ? I'm going to scrape " + scraperUrl);

        PrometheusScraper scraper = new PrometheusScraper(url);
        scraper.scrape(walker);
    }
}