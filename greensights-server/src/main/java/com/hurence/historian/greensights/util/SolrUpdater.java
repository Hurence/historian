package com.hurence.historian.greensights.util;


import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Measure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;


/**
 * Will send chunks to SolR as a background thread.
 */
@Component
public class SolrUpdater {
    private static Logger log = LogManager.getLogger(SolrUpdater.class);

    private final UpdateRequest req = new UpdateRequest();
    private final MeasuresToChunkVersionCurrent converter = new MeasuresToChunkVersionCurrent("prometheus-scrapper");

    private volatile int batchedUpdates = 0;
    private volatile long lastTS = System.currentTimeMillis() * 100; // far in the future ...

    @Value("${historian.solr.collection}")
    private String collection;

    @Value("${historian.solr.batchSize}")
    private Integer batchSize;

    @Value("${historian.solr.flushIntervalMs}")
    private Long flushIntervalMs;

    @Value("${historian.solr.chunkOrigin}")
    private String chunkOrigin;

    @Autowired
    private SolrClient solrClient;

    @Autowired
    private ConcurrentLinkedQueue<Measure> updateQueue;


    @Bean
    public Executor solrUpdaterExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("Greensights-");
        executor.initialize();

        for (int i = 0; i < 8; i++) {
            SolrUpdaterThread updaterThread = new SolrUpdaterThread(collection, batchSize, flushIntervalMs, solrClient, updateQueue, chunkOrigin);
            executor.execute(updaterThread);
        }


        return executor;
    }
}