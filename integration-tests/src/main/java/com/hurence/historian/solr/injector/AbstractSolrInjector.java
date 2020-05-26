package com.hurence.historian.solr.injector;

import com.hurence.historian.solr.util.SolrITHelper;
import com.hurence.historian.spark.compactor.job.ChunkModele;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSolrInjector<T extends ChunkModele> implements SolrInjector {

    private static String COLLECTION = SolrITHelper.COLLECTION_HISTORIAN;
    private List<T> extraCustomChunks = new ArrayList<>();
    
    @Override
    public void injectChunks(SolrClient client) throws SolrServerException, IOException {
        final List<T> chunks = buildListOfChunks();
        chunks.addAll(extraCustomChunks);
        for(int i = 0; i < chunks.size(); i++) {
            T chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected, "id" + i));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION, true, true);
    }

    public void addChunk(T chunk) {
        extraCustomChunks.add(chunk);
    }

    public void addChunk(AbstractSolrInjector injector) {
        extraCustomChunks.addAll(injector.buildListOfChunks());
    }

    protected abstract List<T> buildListOfChunks();

    private SolrInputDocument buildSolrDocument(T chunk, String id) {
        return chunk.buildSolrDocument(id);
    }
}
