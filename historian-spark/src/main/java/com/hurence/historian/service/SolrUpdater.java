/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.historian.service;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class SolrUpdater implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(SolrUpdater.class);

    private final UpdateRequest req = new UpdateRequest();
    private final SolrClient solr;
    private final String solrCollection;
    private final BlockingQueue<SolrInputDocument> records;
    private final int batchSize;
    private final long flushInterval;
    private final AtomicInteger batchedUpdates = new AtomicInteger(0);
    private volatile long lastTS = System.currentTimeMillis();
    private volatile boolean shutdown = false;

    public SolrUpdater(SolrClient solr, String solrCollection, BlockingQueue<SolrInputDocument> records, int batchSize, long flushInterval) {
        this.solr = solr;
        this.records = records;
        this.solrCollection = solrCollection;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
    }

    @Override
    public void run() {
        while (!shutdown) {

            // process record if one
            try {
                if (!records.isEmpty()) {
                    SolrInputDocument record = records.take();
                    req.add(record, 100);
                    batchedUpdates.incrementAndGet();
                }

                long currentTS = System.currentTimeMillis();
                if ((currentTS - lastTS) >= flushInterval || batchedUpdates.get() >= batchSize) {
                    if(req.getDocuments() != null && req.getDocuments().size() >0) {
                        logger.info("processing batch of " + req.getDocuments().size() + " docs for collection " + solrCollection);
                        req.process(solr, solrCollection);
                        req.commit(solr, solrCollection);
                        req.clear();
                    }
                    lastTS = currentTS;
                    batchedUpdates.set(0);
                }

                Thread.sleep(5);
            } catch (IOException | SolrServerException | InterruptedException e) {
                logger.error(e.getMessage());
            }
        }



    }

    public void shutdown() {
        try {
            if(req.getDocuments() != null) {
                req.process(solr, solrCollection);
                req.clear();
                Thread.sleep(500);
            }

        } catch (SolrServerException | InterruptedException | IOException e) {
            logger.error(e.getMessage());
        } finally {
            shutdown = true;
        }

    }

}