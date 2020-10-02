package com.hurence.historian.solr.injector;

import com.hurence.historian.solr.util.SolrITHelper;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractSolrInjector<T> implements SolrInjector {

    private static String COLLECTION = SolrITHelper.COLLECTION_HISTORIAN;
    private List<T> extraCustomChunks = new ArrayList<>();
    
    @Override
    public void injectChunks(SolrClient client) throws SolrServerException, IOException {
        final List<T> chunks = buildListOfChunks();
        chunks.addAll(extraCustomChunks);
        for(int i = 0; i < chunks.size(); i++) {
            T chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION, true, true);
        if (updateRsp.getStatus() != 0) {
            throw new RuntimeException("injection failed status is not 0 ! Response is :" + updateRsp.getResponse().toString());
        }
    }

    public void addChunks(Collection<T> chunks) {
        extraCustomChunks.addAll(chunks);
    }

    public void addChunk(T chunk) {
        extraCustomChunks.add(chunk);
    }

    public void addChunk(AbstractSolrInjector injector) {
        extraCustomChunks.addAll(injector.buildListOfChunks());
    }

    protected abstract List<T> buildListOfChunks();

    abstract protected SolrInputDocument buildSolrDocument(T chunk);


}
