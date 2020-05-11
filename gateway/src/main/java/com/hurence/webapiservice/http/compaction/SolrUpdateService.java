/*
 * Copyright (C) 2018 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.webapiservice.http.compaction;

import org.apache.lucene.document.Document;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import java.io.IOException;
import java.util.Collection;


import static com.hurence.logisland.timeseries.Schema.ID;
import static java.util.stream.Collectors.joining;

/**
 * Executes update actions on solr.
 *
 * @author alex.christ
 */
public class SolrUpdateService {
    private static final int COMMIT_WITHIN = 900000;
    private UpdateRequestProcessor updateProcessor;
    private SolrQueryRequest req;

    /**
     * Creates a new instance.
     *
     * @param req the solr request
     * @param rsp the solr response
     */
    public SolrUpdateService(SolrQueryRequest req, SolrQueryResponse rsp) {
        this(req, req.getCore()
                .getUpdateProcessorChain(req.getParams())
                .createProcessor(req, rsp));
    }

    /**
     * Creates a new instance, mainly for testing purposes
     *
     * @param req             the solr query request
     * @param updateProcessor the update processor
     */
    SolrUpdateService(SolrQueryRequest req, UpdateRequestProcessor updateProcessor) {
        this.updateProcessor = updateProcessor;
        this.req = req;
    }

    /**
     * Adds the given document to the solr index without commit.
     *
     * @param doc the document to add
     * @throws IOException iff something goes wrong
     */
    public void add(SolrInputDocument doc) throws IOException {
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = doc;
        cmd.commitWithin = COMMIT_WITHIN;
        updateProcessor.processAdd(cmd);
    }

    /**
     * Adds the given document to the solr index without commit.
     *
     * @param docs the documents to add
     * @throws IOException iff something goes wrong
     */
    public void add(Collection<SolrInputDocument> docs) throws IOException {
        for (SolrInputDocument doc : docs) {
            AddUpdateCommand cmd = new AddUpdateCommand(req);
            cmd.solrDoc = doc;
            cmd.commitWithin = COMMIT_WITHIN;
            updateProcessor.processAdd(cmd);
        }
    }

    /**
     * Commits open changes to the solr index. Does not optimize the index afterwards.
     *
     * @throws IOException iff something goes wrong
     */
    public void commit() throws IOException {
        updateProcessor.processCommit(new CommitUpdateCommand(req, false));
    }

    /**
     * Deletes documents identified by the given documents.
     *
     * @param docs the documents
     * @throws IOException iff something goes wrong
     */
    public void delete(Collection<Document> docs) throws IOException {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
        cmd.commitWithin = COMMIT_WITHIN;
        cmd.setFlags(DeleteUpdateCommand.BUFFERING);
        cmd.setQuery("{!terms f=" + ID + "}" + docs.stream().map(it -> it.get(ID)).collect(joining(",")));
        updateProcessor.processDelete(cmd);
    }
}
