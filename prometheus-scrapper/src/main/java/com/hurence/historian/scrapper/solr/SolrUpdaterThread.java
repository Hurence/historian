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
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

    @Autowired
    private TaskExecutor taskExecutor;

    @Scheduled(fixedRate = 2)
    public void run() {

        // process measures by
        for (int i = 0; i < 10; i++) {
            try {
                Measure measure = updateQueue.poll(5, TimeUnit.MILLISECONDS);
                if (measure != null) {
                    SolrInputDocument doc = measureToChunkSolrDocument(measure);
                    //  req.add(doc);
                    batchedUpdates++;
                    solrClient.add(collection, doc);
                 //   log.info("updateQueue size {}, batchedUpdates {}", updateQueue.size(), batchedUpdates);
                }
            } catch (InterruptedException | SolrServerException | IOException e) {
                log.error("unable to batch measures to solr request : {}", e.getMessage());
            }
        }


        //
        try {
            long currentTS = System.currentTimeMillis();
            boolean doTimeout = (currentTS - lastTS) >= flushIntervalMs;
            boolean isBatchFull = batchedUpdates > batchSize;
            boolean hasSomethingToCommit = batchedUpdates != 0;

            if ( hasSomethingToCommit && (doTimeout  || isBatchFull )) {
                solrClient.commit(collection);
                log.info("commit updateQueue size {}, batchedUpdates {}, doTimeout {}, isBatchFull {} ",
                        updateQueue.size(),
                        batchedUpdates,
                        doTimeout,
                        isBatchFull);
                lastTS = currentTS;
                batchedUpdates = 0;
            }

        } catch (IOException | SolrServerException e) {
            log.error("unable to send measures to solr : {}", e.getMessage());
        }
    }


    private SolrInputDocument measureToChunkSolrDocument(Measure measure) {
        TreeSet<Measure> measures = new TreeSet<>();
        measures.add(measure);
        String name = measure.getName();
        Map<String, String> tags = measure.getTags();
        Chunk chunk = converter.buildChunk(name, measures, tags);
        return SolrDocumentBuilder.fromChunk(chunk);
    }
}