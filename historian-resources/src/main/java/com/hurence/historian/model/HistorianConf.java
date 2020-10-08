package com.hurence.historian.model;

import com.hurence.historian.model.solr.Schema;
import com.hurence.historian.model.solr.SolrFieldMapping;

public interface HistorianConf {


    static HistorianConf getHistorianConf(SchemaVersion version) {
        return new HistorianConfImpl(version);
    }

    SchemaVersion getVersion();

    Schema getChunkSchema();

    SolrFieldMapping getFieldsInSolr();
}
