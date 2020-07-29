package com.hurence.historian.modele;

import com.hurence.historian.modele.solr.Schema;
import com.hurence.historian.modele.solr.SolrFieldMapping;

public interface HistorianConf {


    static HistorianConf getHistorianConf(SchemaVersion version) {
        return new HistorianConfImpl(version);
    }

    SchemaVersion getVersion();

    Schema getChunkSchema();

//    Schema getReportSchema();
//
//    Schema getAnnotationSchema();

    SolrFieldMapping getFieldsInSolr();
}
