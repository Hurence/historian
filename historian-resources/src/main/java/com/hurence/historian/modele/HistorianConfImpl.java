package com.hurence.historian.modele;

import com.hurence.historian.modele.solr.Schema;
import com.hurence.historian.modele.solr.SolrFieldMapping;

public class HistorianConfImpl implements HistorianConf {

    private SchemaVersion version;
    private Schema chunkSchema;
    private SolrFieldMapping solrFields;

    HistorianConfImpl(SchemaVersion version) {
        this.version = version;
        this.chunkSchema = Schema.getChunkSchema(version);
        this.solrFields = SolrFieldMapping.fromVersion(version);
    }

    @Override
    public SchemaVersion getVersion() {
        return version;
    }

    @Override
    public Schema getChunkSchema() {
        return chunkSchema;
    }

    @Override
    public SolrFieldMapping getFieldsInSolr() {
        return solrFields;
    }
}
