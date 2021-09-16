package com.hurence.historian.scrapper.solr;


import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


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

    @Autowired
    private SolrClient solrClient;

    @Autowired
    private BlockingQueue<Measure> updateQueue;



    @Bean
    public Executor solrUpdaterExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("PrometheusScrapper-");
        executor.initialize();

        SolrUpdaterThread updaterThread = new SolrUpdaterThread(collection, batchSize, flushIntervalMs, solrClient, updateQueue);
        executor.execute(updaterThread);

        return executor;
    }
}