package com.hurence.historian.solr.injector;

import com.hurence.historian.converter.SolrDocumentBuilder;
import com.hurence.historian.modele.SchemaVersion;
import com.hurence.timeseries.model.Chunk;
import org.apache.solr.common.SolrInputDocument;


public abstract class AbstractSolrInjectorChunkCurrentVersion extends AbstractSolrInjector<Chunk> {

    @Override
    protected SolrInputDocument buildSolrDocument(Chunk chunk) {
        return SolrDocumentBuilder.fromChunk(chunk);
    }

    @Override
    public SchemaVersion getVersion() {
        return SchemaVersion.VERSION_1;
    }
}
