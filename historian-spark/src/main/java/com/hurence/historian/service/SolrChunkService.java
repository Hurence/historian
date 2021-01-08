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


import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.timeseries.model.Chunk;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * This class acts as a bulk indexer for chunks
 */
public class SolrChunkService {
    private final static Logger logger = LogManager.getLogger(SolrChunkService.class);


    protected volatile SolrClient solrClient;
    protected List<SolrUpdater> updaters;
    final BlockingQueue<SolrInputDocument> queue = new ArrayBlockingQueue<>(100000);


    /**
     * main constructor : init SolrClient and a thred pool of updaters
     *
     * @param zkHosts
     * @param collectionName
     */
    public SolrChunkService(String zkHosts, String collectionName, int numConcurrentRequests, int batchSize, int flushInterval) {
        logger.info("creating solr client for " + zkHosts);
        solrClient = SolrSupport.getNewSolrCloudClient(zkHosts);

        logger.info("setup a thread pool of " + numConcurrentRequests + " solr updaters");
        updaters = new ArrayList<>(numConcurrentRequests);
        for (int i = 0; i < numConcurrentRequests; i++) {
            SolrUpdater updater = new SolrUpdater(solrClient, collectionName, queue, batchSize, flushInterval);
            new Thread(updater).start();
            updaters.add(updater);
        }
    }


    /**
     * release connections
     */
    public void close() {

        for (SolrUpdater up : updaters) {
            up.shutdown();
        }

        try {
            logger.info("closing solr client");
            solrClient.close();
        } catch (IOException exception) {
            logger.error(exception.getMessage());
        }
    }

    /**
     * convert Chunk to solDoc and add it to queue
     *
     * @param chunk
     * @throws InterruptedException
     */
    public void put(Chunk chunk) throws InterruptedException {
        SolrInputDocument document = SolrDocumentBuilder.fromChunk(chunk);
        queue.put(document);
    }


}