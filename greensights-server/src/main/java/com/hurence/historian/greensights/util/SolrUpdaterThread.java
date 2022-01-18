package com.hurence.historian.greensights.util;


import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.timeseries.converter.MeasuresToChunkVersionCurrent;
import com.hurence.timeseries.model.Chunk;
import com.hurence.timeseries.model.Measure;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Will send chunks to SolR as a background thread.
 */
public class SolrUpdaterThread implements Runnable {
    private static Logger log = LogManager.getLogger(SolrUpdaterThread.class);


    private final MeasuresToChunkVersionCurrent converter;

    private volatile int batchedUpdates = 0;
    private volatile long lastTS = System.currentTimeMillis() + 2000; // far in the future ...

    private final String collection;
    private final Integer batchSize;
    private final Long flushIntervalMs;
    private final SolrClient solrClient;

    //private BlockingQueue<Measure> updateQueue;
    private ConcurrentLinkedQueue<Measure> updateQueue;
    private List<SolrInputDocument> buffer = new ArrayList<>();


    public SolrUpdaterThread(String collection,
                             Integer batchSize,
                             Long flushIntervalMs,
                             SolrClient solrClient,
                             ConcurrentLinkedQueue<Measure> updateQueue,
                             String chunkOrigin) {
        this.collection = collection;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.solrClient = solrClient;
        this.updateQueue = updateQueue;
        this.converter = new MeasuresToChunkVersionCurrent(chunkOrigin);
    }

    @SneakyThrows
    @Override
    public void run() {


        while (true) {

            // convert last measure to solr docs and add it to the indexing buffer
            Measure measure = null;
            try {
                measure = updateQueue.poll();
                if (measure != null) {
                    SolrInputDocument doc = measureToChunkSolrDocument(measure);
                    batchedUpdates++;
                    buffer.add(doc);
                }
            } catch (Exception e) {
                log.error("unable to add measure {} to solr request : {}", measure, e.getMessage());
            }


            // let's check if the batch of docs is ready to be indexed to solr
            try {
                long currentTS = System.currentTimeMillis();
                boolean doTimeout = (currentTS - lastTS) >= flushIntervalMs;
                boolean isBatchFull = batchedUpdates > batchSize;
                boolean hasSomethingToCommit = batchedUpdates != 0;

                if (hasSomethingToCommit && (doTimeout || isBatchFull)) {
                    solrClient.add(collection, buffer,500 );
                    buffer.clear();
                    //solrClient.commit(collection);
                    log.info("commit updateQueue size {}, batchedUpdates {}, doTimeout {}, isBatchFull {} ",
                            updateQueue.size(),
                            batchedUpdates,
                            doTimeout,
                            isBatchFull);
                    lastTS = currentTS;
                    batchedUpdates = 0;
                }

            } catch (Exception  e) {
                log.error("unable to send measures to solr : {}", e.getMessage());
            }
            Thread.sleep(5);
        }
    }


    // TODO we should be able to compact old chunks directly here
    private SolrInputDocument measureToChunkSolrDocument(Measure measure) {
        TreeSet<Measure> measures = new TreeSet<>();
        measures.add(measure);
        String name = measure.getName();
        Map<String, String> tags = measure.getTags();
        Chunk chunk = converter.buildChunk(name, measures, tags);
        chunk.setOrigin("greensights");
        return SolrDocumentBuilder.fromChunk(chunk);
    }
}